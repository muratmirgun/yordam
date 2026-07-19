package secret_test

import (
	"errors"
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestRegistryRetirementRejectsNewProducersButRetainsBufferedStreams(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire(protocol.RuntimeGenerationID("generation-a"), [][]byte{[]byte("leased-secret")})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := lease.Stream()
	if err != nil {
		t.Fatal(err)
	}
	if stream.Write([]byte("leased-sec")) {
		t.Fatal("stream detected an incomplete variant")
	}
	if err := registry.Retire("generation-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire("generation-a", nil); !errors.Is(err, secret.ErrGenerationRetired) {
		t.Fatalf("retired acquire error=%v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if !stream.Write([]byte("ret")) {
		t.Fatal("retirement discarded variants before the buffered stream closed")
	}
	if !stream.Close() {
		t.Fatal("closed stream forgot the detected state")
	}
}

func TestRegistryLeaseRedactsEncodedVariants(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-a", [][]byte{[]byte("encoded-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	for _, value := range admissionVariants([]byte("encoded-secret")) {
		if got := lease.String("before " + string(value) + " after"); got != "before [REDACTED] after" {
			t.Fatalf("redacted value=%q for %q", got, value)
		}
	}
}

func TestClosedLeaseFailsClosedAndRevokesOpenStreams(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-a", [][]byte{[]byte("leased-secret")})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := lease.Stream()
	if err != nil {
		t.Fatal(err)
	}
	if stream.Write([]byte("leased-sec")) {
		t.Fatal("partial secret was detected before revocation")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if !stream.Write([]byte("ret")) || !stream.Close() {
		t.Fatal("revoked stream did not fail closed")
	}
	if got := lease.String("public-output"); got != "[REDACTED]" {
		t.Fatalf("closed lease String=%q", got)
	}
	if got := string(lease.Bytes([]byte("public-output"))); got != "[REDACTED]" {
		t.Fatalf("closed lease Bytes=%q", got)
	}
	if _, err := lease.JSON(map[string]string{"value": "public-output"}); !errors.Is(err, secret.ErrLeaseClosed) {
		t.Fatalf("closed lease JSON error=%v", err)
	}
	if _, err := lease.Derive(); !errors.Is(err, secret.ErrLeaseClosed) {
		t.Fatalf("closed lease Derive error=%v", err)
	}
}

func TestRetiredGenerationKeepsExistingDerivedProducerButRejectsNewOne(t *testing.T) {
	registry := secret.NewRegistry()
	owner, err := registry.Acquire("generation-a", [][]byte{[]byte("leased-secret")})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := owner.Derive()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Retire("generation-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Derive(); !errors.Is(err, secret.ErrGenerationRetired) {
		t.Fatalf("derive after retirement error=%v", err)
	}
	if got := producer.String("before leased-secret after"); got != "before [REDACTED] after" {
		t.Fatalf("existing producer redaction=%q", got)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := producer.String("before leased-secret after"); got != "before [REDACTED] after" {
		t.Fatalf("derived producer lost retired generation=%q", got)
	}
	if err := producer.Close(); err != nil {
		t.Fatal(err)
	}
}
