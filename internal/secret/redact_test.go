package secret_test

import (
	"encoding/json"
	"testing"

	"github.com/muratmirgun/yordam/internal/secret"
)

func TestRedactorRemovesOverlappingSecretsAndIgnoresEmptyValues(t *testing.T) {
	redactor := secret.New("", "abc", "abcdef")

	if got := redactor.String("x abcdef abc y"); got != "x [REDACTED] [REDACTED] y" {
		t.Fatalf("got=%q", got)
	}
}

func TestRedactorJSONRedactsNestedValuesWithoutRewritingKeys(t *testing.T) {
	const value = "quoted-\"secret\""
	redactor := secret.New(value)

	raw, err := redactor.JSON(map[string]any{
		value: "schema value remains",
		"nested": []any{
			map[string]any{"message": "before " + value + " after"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[value] != "schema value remains" {
		t.Fatalf("schema key was rewritten: %#v", decoded)
	}
	nested := decoded["nested"].([]any)[0].(map[string]any)
	if nested["message"] != "before [REDACTED] after" {
		t.Fatalf("nested value=%q", nested["message"])
	}
}

func TestRedactorJSONPreservesLargeIntegers(t *testing.T) {
	const sequence = ^uint64(0)
	raw, err := secret.New("secret").JSON(map[string]any{"sequence": sequence})
	if err != nil {
		t.Fatal(err)
	}

	var decoded struct {
		Sequence uint64 `json:"sequence"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Sequence != sequence {
		t.Fatalf("sequence=%d want=%d raw=%s", decoded.Sequence, sequence, raw)
	}
}
