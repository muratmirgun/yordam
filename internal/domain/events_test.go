package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
)

func TestDurableEventValidate(t *testing.T) {
	t.Parallel()
	event := domain.DurableEvent{
		SchemaVersion: 1,
		EventID:       "01J00000000000000000000000",
		SessionID:     "01J00000000000000000000001",
		Seq:           1,
		Time:          time.Unix(0, 0).UTC(),
		Kind:          domain.EventSessionCreated,
		Payload:       json.RawMessage(`{"workspace":"/tmp/app"}`),
	}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	event.Seq = 0
	if err := event.Validate(); err == nil {
		t.Fatal("zero sequence accepted")
	}
}
