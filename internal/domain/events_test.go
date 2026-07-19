package domain_test

import (
	"encoding/json"
	"reflect"
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

func TestDurableEventV1WireEnvelopeRemainsExact(t *testing.T) {
	t.Parallel()
	event := domain.DurableEvent{
		SchemaVersion: 1,
		EventID:       "event",
		SessionID:     "session",
		Seq:           1,
		Time:          time.Unix(1, 0).UTC(),
		Kind:          domain.EventSessionCreated,
		Payload:       json.RawMessage(`{"workspace":"/tmp/app"}`),
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(object))
	for key := range object {
		got = append(got, key)
	}
	want := []string{"event_id", "kind", "payload", "schema_version", "seq", "session_id", "time"}
	slicesSort(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("v1 envelope keys=%v want=%v", got, want)
	}
	const wantWire = `{"schema_version":1,"event_id":"event","session_id":"session","seq":1,"time":"1970-01-01T00:00:01Z","kind":"session.created","payload":{"workspace":"/tmp/app"}}`
	if string(raw) != wantWire {
		t.Fatalf("v1 envelope wire=%s want=%s", raw, wantWire)
	}
	typeOf := reflect.TypeOf(event)
	if typeOf.NumField() != len(want) {
		t.Fatalf("v1 envelope field count=%d want=%d", typeOf.NumField(), len(want))
	}
}

func slicesSort(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
