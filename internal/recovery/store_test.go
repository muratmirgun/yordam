package recovery_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestRecoveryMaterialRejectsRegisteredEncodedSecretBeforeDiskWrite(t *testing.T) {
	raw := []byte("synthetic-secret")
	for name, value := range recoveryVariants(raw) {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "not-created")
			lease := recoveryLease(t, raw)
			store, err := recovery.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			candidate := validRecoveryCandidate(value)
			if _, err := store.Put(context.Background(), candidate); !errors.Is(err, recovery.ErrSecretDetected) {
				t.Fatalf("value accepted: %x err=%v", value, err)
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("recovery root was written before rejection: %v", err)
			}
		})
	}
}

func TestRecoveryRejectsPreimageDigestMismatchBeforeDiskWrite(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-created")
	store, err := recovery.New(root, recoveryLease(t))
	if err != nil {
		t.Fatal(err)
	}
	candidate := validRecoveryCandidate([]byte("preimage"))
	candidate.PreimageDigest = digestBytes([]byte("different"))
	if _, err := store.Put(context.Background(), candidate); !errors.Is(err, recovery.ErrDigestMismatch) {
		t.Fatalf("put error=%v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("recovery root was written before rejection: %v", err)
	}
}

func TestRecoveryUsesConfidentialModesAndOptionalSealer(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	sealer := recovery.SealerFunc(func(_ context.Context, raw []byte) ([]byte, error) {
		sealed := append([]byte("sealed:"), raw...)
		for left, right := len("sealed:"), len(sealed)-1; left < right; left, right = left+1, right-1 {
			sealed[left], sealed[right] = sealed[right], sealed[left]
		}
		return sealed, nil
	})
	store, err := recovery.New(root, recoveryLease(t), recovery.WithSealer(sealer))
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Put(context.Background(), validRecoveryCandidate([]byte("private-preimage")))
	if err != nil {
		t.Fatal(err)
	}
	if !record.Body.Sealed {
		t.Fatalf("record=%+v", record)
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() && info.Mode().Perm() != 0o700 {
			t.Fatalf("directory %s mode=%o", path, info.Mode().Perm())
		}
		if entry.Type().IsRegular() {
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("file %s mode=%o", path, info.Mode().Perm())
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(raw, []byte("private-preimage")) {
				t.Fatalf("unsealed preimage found in %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRejectsSecretMetadataBeforeCreatingRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-created")
	lease := recoveryLease(t, []byte("identity-secret"))
	store, err := recovery.New(root, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	candidate := validRecoveryCandidate([]byte("safe-preimage"))
	candidate.Subject.ID = "aWRlbnRpdHktc2VjcmV0"
	if _, err := store.Put(context.Background(), candidate); !errors.Is(err, recovery.ErrSecretDetected) {
		t.Fatalf("metadata error=%v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("rejected metadata created root: %v", err)
	}
}

func TestRecoveryAdmitsFinalCanonicalContainerBeforeCreatingRoot(t *testing.T) {
	for name, generatedOnly := range map[string][]byte{
		"created_at":      []byte(`"created_at"`),
		"material_digest": []byte(`"material_digest"`),
		"sealed":          []byte(`"sealed"`),
	} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "not-created")
			lease := recoveryLease(t, generatedOnly)
			store, err := recovery.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err := store.Put(context.Background(), validRecoveryCandidate([]byte("final-container-preimage"))); !errors.Is(err, recovery.ErrSecretDetected) {
				t.Fatalf("final container admission error=%v", err)
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("rejected final container created storage: %v", err)
			}
		})
	}
}

func TestRecoverySingleContainerRetryReconcilesEveryFault(t *testing.T) {
	for _, point := range []recovery.FaultPoint{
		recovery.FaultTemporarySynced,
		recovery.FaultBeforePublish,
		recovery.FaultAfterPublish,
		recovery.FaultDirectorySynced,
	} {
		t.Run(string(point), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "recovery")
			lease := recoveryLease(t)
			crash := errors.New("simulated crash")
			failed := false
			faulting, err := recovery.New(root, lease, recovery.WithFault(func(got recovery.FaultPoint) error {
				if got == point && !failed {
					failed = true
					return crash
				}
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			candidate := validRecoveryCandidate([]byte("retry-preimage"))
			_, _ = faulting.Put(context.Background(), candidate)
			_ = faulting.Close()
			restarted, err := recovery.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			first, err := restarted.Put(context.Background(), candidate)
			if err != nil {
				t.Fatal(err)
			}
			second, err := restarted.Put(context.Background(), candidate)
			if err != nil || first.ID != second.ID {
				t.Fatalf("retry first=%+v second=%+v err=%v", first, second, err)
			}
			entries, err := os.ReadDir(filepath.Join(root, "materials"))
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || filepath.Ext(entries[0].Name()) != ".recovery" {
				t.Fatalf("publication files=%v", entries)
			}
		})
	}
}

func TestRecoveryCoordinationPreventsSecondStoreFromDeletingLiveTemporary(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	writer, err := recovery.New(root, recoveryLease(t), recovery.WithFault(func(point recovery.FaultPoint) error {
		if point == recovery.FaultTemporarySynced {
			close(entered)
			<-release
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	contender, err := recovery.New(root, recoveryLease(t))
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	candidate := validRecoveryCandidate([]byte("coordinated-preimage"))
	type outcome struct {
		record protocol.RecoveryMaterialRecord
		err    error
	}
	finished := make(chan outcome, 1)
	go func() {
		record, putErr := writer.Put(context.Background(), candidate)
		finished <- outcome{record: record, err: putErr}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not reach synced temporary gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := contender.Put(ctx, candidate); !errors.Is(err, recovery.ErrRecoveryBusy) {
		t.Fatalf("contender error=%v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "materials"))
	if err != nil {
		t.Fatal(err)
	}
	if countRecoveryEntries(entries, ".tmp") != 1 || countRecoveryEntries(entries, ".recovery") != 0 {
		t.Fatalf("live writer files=%v", entries)
	}
	close(release)
	first := <-finished
	if first.err != nil {
		t.Fatal(first.err)
	}
	second, err := contender.Put(context.Background(), candidate)
	if err != nil || second.ID != first.record.ID {
		t.Fatalf("reconciled retry=(%+v, %v), writer=%+v", second, err, first.record)
	}
	assertOneRecoveryContainerNoTemporary(t, root)
}

func TestRecoveryCoordinationAcrossProcessesAndOwnerDeath(t *testing.T) {
	for _, mode := range []string{"live", "crash"} {
		t.Run(mode, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "recovery")
			ready := filepath.Join(t.TempDir(), "ready")
			release := filepath.Join(t.TempDir(), "release")
			command := exec.Command(os.Args[0], "-test.run=^TestRecoveryCoordinationSubprocessHelper$")
			command.Env = append(os.Environ(),
				"YORDAM_RECOVERY_HELPER=1",
				"YORDAM_RECOVERY_ROOT="+root,
				"YORDAM_RECOVERY_READY="+ready,
				"YORDAM_RECOVERY_RELEASE="+release,
				"YORDAM_RECOVERY_MODE="+mode,
			)
			var helperOutput bytes.Buffer
			command.Stdout = &helperOutput
			command.Stderr = &helperOutput
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = os.WriteFile(release, []byte("release"), 0o600)
				_ = command.Process.Kill()
			})
			waitForRecoveryHelperFile(t, ready)
			contender, err := recovery.New(root, recoveryLease(t))
			if err != nil {
				t.Fatal(err)
			}
			defer contender.Close()
			candidate := validRecoveryCandidate([]byte("subprocess-preimage"))
			if mode == "live" {
				ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
				_, putErr := contender.Put(ctx, candidate)
				cancel()
				if !errors.Is(putErr, recovery.ErrRecoveryBusy) {
					t.Fatalf("live subprocess contender error=%v", putErr)
				}
				if err := os.WriteFile(release, []byte("release"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := command.Wait(); err != nil {
					t.Fatalf("live helper: %v: %s", err, helperOutput.Bytes())
				}
			} else {
				if err := command.Wait(); err == nil {
					t.Fatal("crash helper exited successfully")
				}
			}
			if _, err := contender.Put(context.Background(), candidate); err != nil {
				t.Fatalf("owner-death retry: %v", err)
			}
			assertOneRecoveryContainerNoTemporary(t, root)
			info, err := os.Stat(filepath.Join(root, ".recovery.lock"))
			if err != nil || info.Mode().Perm() != 0o600 {
				t.Fatalf("lock info=(%v, %v)", info, err)
			}
		})
	}
}

func TestRecoveryCoordinationSubprocessHelper(t *testing.T) {
	if os.Getenv("YORDAM_RECOVERY_HELPER") != "1" {
		return
	}
	root := os.Getenv("YORDAM_RECOVERY_ROOT")
	ready := os.Getenv("YORDAM_RECOVERY_READY")
	release := os.Getenv("YORDAM_RECOVERY_RELEASE")
	mode := os.Getenv("YORDAM_RECOVERY_MODE")
	store, err := recovery.New(root, recoveryLease(t), recovery.WithFault(func(point recovery.FaultPoint) error {
		if point != recovery.FaultTemporarySynced {
			return nil
		}
		if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
			return err
		}
		if mode == "crash" {
			os.Exit(88)
		}
		for {
			if _, err := os.Stat(release); err == nil {
				return nil
			} else if !os.IsNotExist(err) {
				return err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Put(context.Background(), validRecoveryCandidate([]byte("subprocess-preimage"))); err != nil {
		t.Fatal(err)
	}
}

func waitForRecoveryHelperFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("recovery subprocess did not reach synced temporary gate")
}

func countRecoveryEntries(entries []os.DirEntry, suffix string) int {
	count := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), suffix) {
			count++
		}
	}
	return count
}

func assertOneRecoveryContainerNoTemporary(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "materials"))
	if err != nil {
		t.Fatal(err)
	}
	if countRecoveryEntries(entries, ".recovery") != 1 || countRecoveryEntries(entries, ".tmp") != 0 {
		t.Fatalf("publication files=%v", entries)
	}
}

func TestRecoveryCandidateCannotSerializeOrDisplayRawPreimage(t *testing.T) {
	candidate := validRecoveryCandidate([]byte("do-not-display"))
	if raw, err := json.Marshal(candidate); err == nil || bytes.Contains(raw, candidate.Preimage) {
		t.Fatalf("marshal=(%q, %v)", raw, err)
	}
	for _, formatted := range []string{fmt.Sprint(candidate), fmt.Sprintf("%+v", candidate), fmt.Sprintf("%#v", candidate)} {
		if strings.Contains(formatted, "do-not-display") || strings.Contains(formatted, fmt.Sprint([]byte("do-not-display"))) {
			t.Fatalf("candidate display exposed preimage: %s", formatted)
		}
	}
}

func TestRecoveryPinsExistingRootOrDeepestAncestorAtConstruction(t *testing.T) {
	for _, target := range []string{"existing_root", "deepest_existing_ancestor"} {
		t.Run(target, func(t *testing.T) {
			base := t.TempDir()
			anchor := filepath.Join(base, "anchor")
			if err := os.Mkdir(anchor, 0o700); err != nil {
				t.Fatal(err)
			}
			root := anchor
			if target == "deepest_existing_ancestor" {
				root = filepath.Join(anchor, "missing", "recovery")
			}
			store, err := recovery.New(root, recoveryLease(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if target == "deepest_existing_ancestor" {
				if _, err := os.Stat(root); !os.IsNotExist(err) {
					t.Fatalf("constructor created confidential root: %v", err)
				}
			}
			moved := anchor + "-retained"
			if err := os.Rename(anchor, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(anchor, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Put(context.Background(), validRecoveryCandidate([]byte("confidential"))); !errors.Is(err, recovery.ErrUnsafePath) {
				t.Fatalf("construction identity replacement error=%v", err)
			}
			for _, candidateRoot := range []string{root, filepath.Join(moved, "missing", "recovery")} {
				if raw, err := os.ReadDir(candidateRoot); err == nil && len(raw) != 0 {
					t.Fatalf("rejected put populated %q", candidateRoot)
				} else if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRecoveryRejectsInRootSymlinkSwapOfPinnedMaterials(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	store, err := recovery.New(root, recoveryLease(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	candidate := validRecoveryCandidate([]byte("retained-material"))
	if _, err := store.Put(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	materials := filepath.Join(root, "materials")
	retained := materials + "-retained"
	if err := os.Rename(materials, retained); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(retained), materials); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), candidate); !errors.Is(err, recovery.ErrUnsafePath) {
		t.Fatalf("existing store materials swap error=%v", err)
	}
	fresh, err := recovery.New(root, recoveryLease(t))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, err := fresh.Put(context.Background(), candidate); !errors.Is(err, recovery.ErrUnsafePath) {
		t.Fatalf("fresh store materials swap error=%v", err)
	}
}

func validRecoveryCandidate(preimage []byte) recovery.Candidate {
	return recovery.Candidate{
		WorkspaceID: "workspace-a", ActivityID: "activity-a", CheckpointID: "checkpoint-a",
		Subject: protocol.SubjectRef{Kind: "file", ID: "subject.txt"}, PlanDigest: digestBytes([]byte("plan")),
		Preimage: append([]byte(nil), preimage...), PreimageDigest: digestBytes(preimage),
		ExpectedPostimageDigest: digestBytes([]byte("postimage")), Mode: 0o644,
	}
}

func recoveryLease(t *testing.T, values ...[]byte) *secret.Lease {
	t.Helper()
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("recovery-test", values)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	return lease
}

func digestBytes(value []byte) protocol.Digest {
	sum := sha256.Sum256(value)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}
}

func recoveryVariants(raw []byte) map[string][]byte {
	lower := hex.EncodeToString(raw)
	return map[string][]byte{
		"raw":              raw,
		"base64-padded":    []byte(base64.StdEncoding.EncodeToString(raw)),
		"base64-raw":       []byte(base64.RawStdEncoding.EncodeToString(raw)),
		"base64url-padded": []byte(base64.URLEncoding.EncodeToString(raw)),
		"base64url-raw":    []byte(base64.RawURLEncoding.EncodeToString(raw)),
		"hex-lower":        []byte(lower),
		"hex-upper":        []byte(strings.ToUpper(lower)),
	}
}
