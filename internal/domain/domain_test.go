package domain_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
)

func TestPermissionModeValidate(t *testing.T) {
	t.Parallel()
	for _, mode := range []domain.PermissionMode{domain.ModeSafe, domain.ModeAsk, domain.ModeAuto} {
		if err := mode.Validate(); err != nil {
			t.Fatalf("%q: %v", mode, err)
		}
	}
	if err := domain.PermissionMode("root").Validate(); err == nil {
		t.Fatal("invalid mode accepted")
	}
}

func TestToolDescriptorValidate(t *testing.T) {
	t.Parallel()
	d := domain.ToolDescriptor{
		Name:             "read",
		Description:      "Read UTF-8 text",
		ScopeDescription: "Exact canonical file path",
		InputSchema:      json.RawMessage(`{"type":"object"}`),
		Mutation:         domain.MutationReadOnly,
	}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	d.Name = "Read File"
	if err := d.Validate(); err == nil {
		t.Fatal("invalid tool name accepted")
	}
}

func TestToolDescriptorValidateRejectsInvalidDescriptors(t *testing.T) {
	t.Parallel()

	valid := domain.ToolDescriptor{
		Name:             "read",
		Description:      "Read UTF-8 text",
		ScopeDescription: "Exact canonical file path",
		InputSchema:      json.RawMessage(`{"type":"object"}`),
		Mutation:         domain.MutationReadOnly,
	}
	tests := map[string]domain.ToolDescriptor{
		"empty description": func() domain.ToolDescriptor {
			d := valid
			d.Description = ""
			return d
		}(),
		"empty scope description": func() domain.ToolDescriptor {
			d := valid
			d.ScopeDescription = ""
			return d
		}(),
		"invalid input schema": func() domain.ToolDescriptor {
			d := valid
			d.InputSchema = json.RawMessage(`{"type":`)
			return d
		}(),
		"invalid mutation kind": func() domain.ToolDescriptor {
			d := valid
			d.Mutation = domain.MutationKind("network")
			return d
		}(),
	}

	for name, descriptor := range tests {
		descriptor := descriptor
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := descriptor.Validate(); err == nil {
				t.Fatal("invalid descriptor accepted")
			}
		})
	}
}

func TestTypedError(t *testing.T) {
	t.Parallel()

	cause := errors.New("provider unavailable")
	err := &domain.TypedError{
		Kind:    domain.ErrorProviderRetryable,
		Message: "request failed",
		Cause:   cause,
	}

	if got := err.Error(); got != err.Message {
		t.Fatalf("Error() = %q, want %q", got, err.Message)
	}
	if !errors.Is(err, cause) {
		t.Fatal("TypedError does not unwrap its cause")
	}
}
