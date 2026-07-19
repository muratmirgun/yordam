package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestApplicationDTOValidationRejectsInvalidCorrelationAndBounds(t *testing.T) {
	digest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}
	command := protocol.Command{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		CommandID:       "command-1",
		Actor:           protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser},
		IdempotencyKey:  "key-1",
		RequestDigest:   digest,
		Kind:            "start_turn",
		PayloadVersion:  1,
		Payload:         json.RawMessage(`{"prompt":"hello"}`),
	}
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}
	command.ProtocolVersion++
	if err := command.Validate(); err == nil {
		t.Fatal("application version mismatch was accepted")
	}

	event := protocol.ApplicationEvent{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		StreamEventID:   "stream-1",
		Time:            time.Unix(1, 0).UTC(),
		Kind:            protocol.EventControlOperationStarted,
		Classification:  "durable",
		PayloadVersion:  1,
		Payload:         json.RawMessage(`{}`),
		Correlation: protocol.EventCorrelation{
			JournalKind: protocol.JournalWorkspaceControl,
			JournalID:   "workspace-1",
			SessionID:   "session-must-not-leak",
		},
	}
	if err := event.Validate(); err == nil {
		t.Fatal("workspace-control event with session ownership was accepted")
	}
	event.Correlation.SessionID = ""
	if err := event.Validate(); err == nil {
		t.Fatal("control-operation event without operation ID was accepted")
	}
}
