package orchestrator

import (
	"fmt"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// validateCompactionProposedEvents validates the native event against its real
// future sequence number. ContextCompactedV1 is intentionally anchored before
// the event that activates it, so the generic sequence-one preflight cannot be
// used for this transaction.
func validateCompactionProposedEvents(events []protocol.ProposedEvent, ref protocol.JournalRef, firstSequence uint64) error {
	return validateProposedEventsAt(events, ref, firstSequence, "validation")
}

func validateProposedEventsAt(events []protocol.ProposedEvent, ref protocol.JournalRef, firstSequence uint64, transactionID protocol.TransactionID) error {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		return err
	}
	for index, event := range events {
		if event.EventID == "" || event.Time.IsZero() || event.PayloadVersion == 0 || event.Kind == "" || event.RuntimeGenerationID == "" || event.Actor == nil || event.Actor.Validate() != nil {
			return fmt.Errorf("proposed event %d identity is incomplete", index)
		}
		if event.SessionID == "" || protocol.JournalID(event.SessionID) != ref.ID || protocol.ValidateRawJSON(event.Payload) != nil {
			return fmt.Errorf("proposed event %d session identity is invalid", index)
		}
		envelope := protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: event.PayloadVersion, JournalKind: ref.Kind, JournalID: ref.ID, EventID: event.EventID, SessionID: event.SessionID, Seq: firstSequence + uint64(index), Time: event.Time, Kind: event.Kind, TaskID: event.TaskID, TurnID: event.TurnID, ActivityID: event.ActivityID, ParentActivityID: event.ParentActivityID, CausationEventID: event.CausationEventID, Actor: protocol.DeepCopy(event.Actor), RuntimeGenerationID: event.RuntimeGenerationID, TransactionID: transactionID, Payload: protocol.DeepCopy(event.Payload)}
		raw, marshalErr := canonicaljson.Marshal(envelope)
		if marshalErr != nil {
			return marshalErr
		}
		record, decodeErr := registry.Decode(raw)
		if decodeErr != nil {
			return decodeErr
		}
		if validateErr := registry.Validate(record); validateErr != nil {
			return validateErr
		}
	}
	return nil
}
