package jsonl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
)

func TestValidateEventLogKeepsOnlySummary(t *testing.T) {
	const sessionID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	var contents bytes.Buffer
	for sequence := uint64(1); sequence <= 3; sequence++ {
		event := domain.DurableEvent{
			SchemaVersion: 1,
			EventID:       sessionID,
			SessionID:     sessionID,
			Seq:           sequence,
			Time:          time.Date(2026, 7, 13, 12, 0, int(sequence), 0, time.UTC),
			Kind:          domain.EventUserMessage,
			Payload:       json.RawMessage(`{"content":"validated"}`),
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		contents.Write(encoded)
		contents.WriteByte('\n')
	}

	summary, err := validateEventLog(context.Background(), bytes.NewReader(contents.Bytes()), sessionID, true)
	if err != nil {
		t.Fatal(err)
	}
	if summary.count != 3 || summary.lastSeq != 3 {
		t.Fatalf("summary=%+v want count=3 last_seq=3", summary)
	}
}

func TestCancellationDuringMetadataRead(t *testing.T) {
	testCanceledFiniteRead(t, maxSessionMetadataBytes, bytes.Repeat([]byte("m"), 48<<10))
}

func TestCancellationDuringRecoveryRead(t *testing.T) {
	testCanceledFiniteRead(t, MaxArtifactBytes, bytes.Repeat([]byte("r"), 96<<10))
}

func TestCancellationDuringEventRead(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "events-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	line := `{"schema_version":1,"event_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-13T12:00:00Z","kind":"user.message","payload":{"content":"` + strings.Repeat("x", 96<<10) + `"}}\n`
	if _, err := file.WriteString(line); err != nil {
		t.Fatal(err)
	}
	ctx := newCancelAfterChecksContext(2)
	_, err = validateEventLog(ctx, file, "01ARZ3NDEKTSV4RRFFQ69G5FAV", true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("validateEventLog error=%v want context canceled", err)
	}
}

func TestCanceledPersistentReadReleasesDescriptor(t *testing.T) {
	directory := t.TempDir()
	before, err := countOpenDescriptors()
	if err != nil {
		t.Skipf("descriptor count unavailable: %v", err)
	}
	for index := range 20 {
		path := directory + "/read-" + string(rune('a'+index))
		if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 48<<10), 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx := newCancelAfterChecksContext(2)
		_, readErr := readOpenedFile(ctx, file, maxSessionMetadataBytes)
		closeErr := file.Close()
		if !errors.Is(readErr, context.Canceled) || closeErr != nil {
			t.Fatalf("iteration %d read=%v close=%v", index, readErr, closeErr)
		}
	}
	after, err := countOpenDescriptors()
	if err != nil {
		t.Fatal(err)
	}
	if after > before+2 {
		t.Fatalf("open descriptors grew from %d to %d", before, after)
	}
}

func testCanceledFiniteRead(t *testing.T, limit int64, contents []byte) {
	t.Helper()
	path := t.TempDir() + "/persistent"
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ctx := newCancelAfterChecksContext(2)
	_, err = readOpenedFile(ctx, file, limit)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("readOpenedFile error=%v want context canceled", err)
	}
}

type cancelAfterChecksContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
	after  int
}

func newCancelAfterChecksContext(after int) *cancelAfterChecksContext {
	parent, cancel := context.WithCancel(context.Background())
	return &cancelAfterChecksContext{Context: parent, cancel: cancel, after: after}
}

func (c *cancelAfterChecksContext) Err() error {
	c.checks++
	if c.checks == c.after {
		c.cancel()
	}
	return c.Context.Err()
}

func countOpenDescriptors() (int, error) {
	count := 0
	for descriptor := 0; descriptor < 4096; descriptor++ {
		var stat syscall.Stat_t
		err := syscall.Fstat(descriptor, &stat)
		if err == nil {
			count++
			continue
		}
		if !errors.Is(err, syscall.EBADF) {
			return 0, err
		}
	}
	return count, nil
}
