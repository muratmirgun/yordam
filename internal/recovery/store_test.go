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
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestRecoveryMaterialRejectsRegisteredEncodedSecretBeforeDiskWrite(t *testing.T) {
	raw := []byte("synthetic-secret")
	for name, value := range recoveryVariants(raw) {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "not-created")
			store, err := recovery.New(root, secret.NewAdmissionScanner(raw))
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
	store, err := recovery.New(root, secret.NewAdmissionScanner())
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
	store, err := recovery.New(root, secret.NewAdmissionScanner(), recovery.WithSealer(sealer))
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

func validRecoveryCandidate(preimage []byte) recovery.Candidate {
	return recovery.Candidate{
		WorkspaceID: "workspace-a", ActivityID: "activity-a", CheckpointID: "checkpoint-a",
		Subject: protocol.SubjectRef{Kind: "file", ID: "subject.txt"}, PlanDigest: digestBytes([]byte("plan")),
		Preimage: append([]byte(nil), preimage...), PreimageDigest: digestBytes(preimage),
		ExpectedPostimageDigest: digestBytes([]byte("postimage")), Mode: 0o644,
	}
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
