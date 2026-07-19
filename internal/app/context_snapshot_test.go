package app

import (
	"github.com/muratmirgun/yordam/internal/protocol"
	"testing"
)

func TestDurableContextFailsClosed(t *testing.T) {
	valid := protocol.ApplicationSnapshot{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", State: protocol.ValueKnown, Data: []byte(`{"revision":"r"}`)}}}
	got, err := DurableContext(valid)
	if err != nil || got == nil || got.Revision != "r" {
		t.Fatalf("%+v %v", got, err)
	}
	for _, snapshot := range []protocol.ApplicationSnapshot{{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "other", State: protocol.ValueKnown}}}, {Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", State: protocol.ValueUnknown}}}} {
		got, err := DurableContext(snapshot)
		if err != nil || got != nil {
			t.Fatalf("%+v %v", got, err)
		}
	}
	malformed := protocol.ApplicationSnapshot{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", State: protocol.ValueKnown, Data: []byte(`{`)}}}
	if _, err := DurableContext(malformed); err == nil {
		t.Fatal("accepted malformed")
	}
}
