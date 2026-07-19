package canonicaljson_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestMarshalSortsObjectsAndRejectsNonIntegerNumbers(t *testing.T) {
	t.Parallel()
	got, err := canonicaljson.Marshal(json.RawMessage(`{"z":1,"a":[true,"x"]}`))
	if err != nil || string(got) != `{"a":[true,"x"],"z":1}` {
		t.Fatalf("canonical=%s err=%v", got, err)
	}
	for _, raw := range []string{`1.5`, `1e3`, `01`, `{"a":1,"a":2}`, `{} []`} {
		if _, err := canonicaljson.Marshal(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted non-canonical input %s", raw)
		}
	}
}

func TestMarshalEnforcesCanonicalBounds(t *testing.T) {
	t.Parallel()
	for _, raw := range []json.RawMessage{
		json.RawMessage(strings.Repeat("[", protocol.MaxJSONDepth+1) + "0" + strings.Repeat("]", protocol.MaxJSONDepth+1)),
		json.RawMessage(`"` + strings.Repeat("x", protocol.MaxStringBytes+1) + `"`),
		json.RawMessage(`[` + strings.Repeat("0,", protocol.MaxCollectionMembers) + `0]`),
	} {
		if _, err := canonicaljson.Marshal(raw); err == nil {
			t.Fatal("accepted value beyond canonical JSON bounds")
		}
	}
}

func TestMarshalDoesNotTreatTopLevelRawJSONAsAByteField(t *testing.T) {
	t.Parallel()
	part := strings.Repeat("x", 600<<10)
	raw := json.RawMessage(`["` + part + `","` + part + `"]`)
	got, err := canonicaljson.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(raw) {
		t.Fatal("canonical output changed already-canonical large JSON")
	}
}

func TestDigestAndTransactionDigestAreDeterministic(t *testing.T) {
	t.Parallel()
	a, err := canonicaljson.Digest(json.RawMessage(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicaljson.Digest(json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a.Algorithm != protocol.DigestSHA256 || len(a.Value) != 64 {
		t.Fatalf("digests differ: %#v %#v", a, b)
	}

	events := []protocol.EventEnvelope{{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: protocol.JournalSession, JournalID: "s", EventID: "e", SessionID: "s",
		Seq: 1, Kind: protocol.EventSessionTitleChanged, TransactionID: "tx",
		Payload: json.RawMessage(`{"title":"x"}`),
	}}
	first, err := canonicaljson.TransactionDigest(events)
	if err != nil {
		t.Fatal(err)
	}
	events[0].Payload[10] = 'y'
	second, err := canonicaljson.TransactionDigest(events)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("transaction digest ignored changed payload")
	}
}

func TestValidateDigestRecomputesBodyOnly(t *testing.T) {
	t.Parallel()
	body := struct {
		Value string `json:"value"`
	}{Value: "x"}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := canonicaljson.ValidateDigest(body, digest); err != nil {
		t.Fatal(err)
	}
	body.Value = "y"
	if err := canonicaljson.ValidateDigest(body, digest); err == nil {
		t.Fatal("stale body digest accepted")
	}
}
