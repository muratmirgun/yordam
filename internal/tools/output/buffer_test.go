package output_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

func TestBufferCapsExcerptAndCreatesRedactedArtifact(t *testing.T) {
	store := &fakeArtifactStore{}
	buffer := output.New(output.Options{SessionID: "s", Artifacts: store, Redact: secret.New("secret")})
	input := strings.Repeat("x", output.ModelExcerptBytes) + "secret" + strings.Repeat("y", 100)
	if written, err := buffer.Write([]byte(input)); err != nil || written != len(input) {
		t.Fatalf("write=(%d, %v) want=(%d, nil)", written, err, len(input))
	}

	result, err := buffer.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result.Content, "secret") || len(result.Content) > output.ModelExcerptBytes {
		t.Fatalf("result=%#v", result)
	}
	if result.Truncated {
		t.Fatalf("retained output unexpectedly marked truncated: %#v", result)
	}
	if len(result.ArtifactIDs) != 1 {
		t.Fatalf("artifacts=%v", result.ArtifactIDs)
	}
	artifacts := store.contents()
	if len(artifacts) != 1 || bytes.Contains(artifacts[0], []byte("secret")) {
		t.Fatalf("unsafe artifacts=%q", artifacts)
	}
}

func TestBufferCapsRetentionAndPreservesWriteContract(t *testing.T) {
	store := &fakeArtifactStore{}
	buffer := output.New(output.Options{SessionID: "s", Artifacts: store})
	input := bytes.Repeat([]byte("x"), output.RetainedBytes+257)

	written, err := buffer.Write(input)
	if err != nil || written != len(input) {
		t.Fatalf("write=(%d, %v) want=(%d, nil)", written, err, len(input))
	}
	result, err := buffer.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || len(result.Content) != output.ModelExcerptBytes {
		t.Fatalf("result=%#v", result)
	}
	artifacts := store.contents()
	if len(artifacts) != 1 || len(artifacts[0]) != output.RetainedBytes {
		t.Fatalf("artifact sizes=%v", artifactSizes(artifacts))
	}
}

func TestBufferUsesRawSizeToDecideWhetherArtifactIsNeeded(t *testing.T) {
	largeSecret := strings.Repeat("s", output.ModelExcerptBytes+1)
	store := &fakeArtifactStore{}
	buffer := output.New(output.Options{SessionID: "s", Artifacts: store, Redact: secret.New(largeSecret)})
	if _, err := buffer.Write([]byte(largeSecret)); err != nil {
		t.Fatal(err)
	}

	result, err := buffer.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Content != "[REDACTED]" || len(result.ArtifactIDs) != 1 {
		t.Fatalf("result=%#v", result)
	}
	if got := store.contents(); len(got) != 1 || string(got[0]) != "[REDACTED]" {
		t.Fatalf("artifacts=%q", got)
	}
}

func TestBufferCreatesArtifactWhenRedactionExpandsPastExcerptLimit(t *testing.T) {
	store := &fakeArtifactStore{}
	buffer := output.New(output.Options{SessionID: "s", Artifacts: store, Redact: secret.New("x")})
	if _, err := buffer.Write(bytes.Repeat([]byte("x"), output.ModelExcerptBytes)); err != nil {
		t.Fatal(err)
	}

	result, err := buffer.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Content) != output.ModelExcerptBytes || len(result.ArtifactIDs) != 1 {
		t.Fatalf("result=%#v", result)
	}
	if got := store.contents(); len(got) != 1 || bytes.Contains(got[0], []byte("x")) {
		t.Fatalf("artifacts=%q", got)
	}
}

func TestBufferUsesCurrentArtifactSessionID(t *testing.T) {
	store := &recordingArtifactStore{}
	current := "session-one"
	buffer := output.New(output.Options{SessionID: "startup", CurrentSessionID: func() string { return current }, Artifacts: store})
	_, _ = buffer.Write(bytes.Repeat([]byte("x"), output.ModelExcerptBytes+1))
	if _, err := buffer.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.sessionID != "session-one" {
		t.Fatalf("artifact session=%q", store.sessionID)
	}
}

type recordingArtifactStore struct{ sessionID string }

func (s *recordingArtifactStore) Put(_ context.Context, sessionID, _ string, source io.Reader, limit int64) (domain.Artifact, error) {
	s.sessionID = sessionID
	_, _ = io.Copy(io.Discard, io.LimitReader(source, limit))
	return domain.Artifact{ID: "artifact", SessionID: sessionID}, nil
}

func (*recordingArtifactStore) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func TestBufferSnapshotIsThreadSafeAndBounded(t *testing.T) {
	buffer := output.New(output.Options{Redact: secret.New("secret")})
	const writers = 16
	var wait sync.WaitGroup
	for range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				chunk := []byte("line secret\n")
				if written, err := buffer.Write(chunk); err != nil || written != len(chunk) {
					t.Errorf("write=(%d, %v)", written, err)
				}
				content, _ := buffer.Snapshot()
				if len(content) > output.ModelExcerptBytes || strings.Contains(content, "secret") {
					t.Errorf("unsafe snapshot bytes=%d", len(content))
				}
			}
		}()
	}
	wait.Wait()
}

func TestBufferKeepsImmutableRedactorSnapshot(t *testing.T) {
	binding := secret.NewBinding(secret.New("old-secret"))
	oldBuffer := output.New(output.Options{Redact: binding.Snapshot()})
	binding.Replace(secret.New("new-secret"))
	newBuffer := output.New(output.Options{Redact: binding.Snapshot()})
	_, _ = oldBuffer.Write([]byte("old-secret new-secret"))
	_, _ = newBuffer.Write([]byte("old-secret new-secret"))
	oldValue, _ := oldBuffer.Snapshot()
	newValue, _ := newBuffer.Snapshot()
	if oldValue != "[REDACTED] new-secret" || newValue != "old-secret [REDACTED]" {
		t.Fatalf("old=%q new=%q", oldValue, newValue)
	}
}

type fakeArtifactStore struct {
	mu        sync.Mutex
	artifacts [][]byte
}

func (s *fakeArtifactStore) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	raw, err := io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		return domain.Artifact{}, err
	}
	if int64(len(raw)) > limit {
		return domain.Artifact{}, fmt.Errorf("artifact exceeded limit: %d > %d", len(raw), limit)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := fmt.Sprintf("artifact-%d", len(s.artifacts)+1)
	s.artifacts = append(s.artifacts, append([]byte(nil), raw...))
	return domain.Artifact{ID: id, SessionID: sessionID, MediaType: mediaType, Size: int64(len(raw))}, nil
}

func (s *fakeArtifactStore) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *fakeArtifactStore) contents() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.artifacts))
	for index, artifact := range s.artifacts {
		out[index] = append([]byte(nil), artifact...)
	}
	return out
}

func artifactSizes(artifacts [][]byte) []int {
	sizes := make([]int, len(artifacts))
	for index, artifact := range artifacts {
		sizes[index] = len(artifact)
	}
	return sizes
}
