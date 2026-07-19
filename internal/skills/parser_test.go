package skills

import (
	"bytes"
	"strings"
	"testing"
)

func validSkill(name, description, body string) []byte {
	return []byte("---\nname: " + name + "\ndescription: " + description + "\n---\n" + body)
}

func TestParseAdmitsStrictMinimalSkill(t *testing.T) {
	raw := validSkill("go-testing", "Test Go code.", "Use go test.\n")
	metadata, content, err := Parse("go-testing", raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if metadata != (Metadata{Name: "go-testing", Description: "Test Go code."}) {
		t.Fatalf("metadata = %#v", metadata)
	}
	if !bytes.Equal(content, raw) {
		t.Fatalf("content = %q, want %q", content, raw)
	}
	raw[0] = 'x'
	if content[0] != '-' {
		t.Fatal("returned content aliases caller input")
	}
}

func TestParseAcceptsPlainProsePunctuation(t *testing.T) {
	metadata, _, err := Parse("go-testing", validSkill("go-testing", "Test, format, and verify Go code.", "Use go test.\n"))
	if err != nil || metadata.Description != "Test, format, and verify Go code." {
		t.Fatalf("Parse() metadata/error = %#v / %v", metadata, err)
	}
}

func TestParseAcceptsKeyOrderAndNormalizesCRLF(t *testing.T) {
	raw := []byte("---\r\ndescription: Test Go code.\r\nname: go-testing\r\n---\r\nBody\r\n")
	metadata, content, err := Parse("go-testing", raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if metadata.Description != "Test Go code." {
		t.Fatalf("description = %q", metadata.Description)
	}
	if bytes.Contains(content, []byte{'\r'}) || string(content) != "---\ndescription: Test Go code.\nname: go-testing\n---\nBody\n" {
		t.Fatalf("content was not normalized: %q", content)
	}
}

func TestParseRejectsInvalidInput(t *testing.T) {
	tooLongDescription := strings.Repeat("d", 1025)
	cases := []struct {
		name string
		dir  string
		raw  []byte
	}{
		{"empty body", "go-testing", validSkill("go-testing", "description", " \t\n")},
		{"missing opening fence", "go-testing", []byte("name: go-testing\ndescription: description\n---\nbody\n")},
		{"unterminated fence", "go-testing", []byte("---\nname: go-testing\ndescription: description\nbody\n")},
		{"missing name", "go-testing", []byte("---\ndescription: description\n---\nbody\n")},
		{"missing description", "go-testing", []byte("---\nname: go-testing\n---\nbody\n")},
		{"duplicate name", "go-testing", []byte("---\nname: go-testing\nname: go-testing\ndescription: description\n---\nbody\n")},
		{"duplicate description", "go-testing", []byte("---\nname: go-testing\ndescription: description\ndescription: again\n---\nbody\n")},
		{"unknown key", "go-testing", []byte("---\nname: go-testing\ndescription: description\nextra: value\n---\nbody\n")},
		{"quoted value", "go-testing", validSkill("go-testing", "\"description\"", "body\n")},
		{"empty value", "go-testing", validSkill("go-testing", "", "body\n")},
		{"escaped value", "go-testing", validSkill("go-testing", "description\\nnext", "body\n")},
		{"alias value", "go-testing", validSkill("go-testing", "*description", "body\n")},
		{"tag value", "go-testing", validSkill("go-testing", "!description", "body\n")},
		{"comment value", "go-testing", validSkill("go-testing", "description # note", "body\n")},
		{"multiline value", "go-testing", []byte("---\nname: go-testing\ndescription: |\n description\n---\nbody\n")},
		{"structural value", "go-testing", validSkill("go-testing", "[description]", "body\n")},
		{"object value", "go-testing", validSkill("go-testing", "{description: value}", "body\n")},
		{"leading value whitespace", "go-testing", []byte("---\nname: go-testing\ndescription:  description\n---\nbody\n")},
		{"trailing value whitespace", "go-testing", []byte("---\nname: go-testing\ndescription: description \n---\nbody\n")},
		{"directory mismatch", "other", validSkill("go-testing", "description", "body\n")},
		{"invalid directory name", "Go-testing", validSkill("Go-testing", "description", "body\n")},
		{"invalid frontmatter name", "go-testing", validSkill("go--testing", "description", "body\n")},
		{"invalid utf8", "go-testing", append(validSkill("go-testing", "description", "body"), 0xff)},
		{"nul", "go-testing", validSkill("go-testing", "description", "body\x00\n")},
		{"binary control", "go-testing", validSkill("go-testing", "description", "body\x01\n")},
		{"delete control in body", "go-testing", validSkill("go-testing", "description", "body\x7f\n")},
		{"c1 control in body", "go-testing", append(validSkill("go-testing", "description", "body "), []byte("\u0085\n")...)},
		{"bare carriage return", "go-testing", []byte("---\rname: go-testing\ndescription: description\n---\nbody\n")},
		{"list scalar", "go-testing", validSkill("go-testing", "- item", "body\n")},
		{"mapping scalar", "go-testing", validSkill("go-testing", "? key", "body\n")},
		{"unicode leading whitespace", "go-testing", validSkill("go-testing", "\u00a0description", "body\n")},
		{"unicode trailing whitespace", "go-testing", validSkill("go-testing", "description\u00a0", "body\n")},
		{"description too long", "go-testing", validSkill("go-testing", tooLongDescription, "body\n")},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := Parse(test.dir, test.raw); err == nil {
				t.Fatal("Parse() error = nil")
			}
		})
	}
}

func TestParseDescriptionBoundary(t *testing.T) {
	_, _, err := Parse("go-testing", validSkill("go-testing", strings.Repeat("d", 1024), "body\n"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
}

func TestParseRejectsOversizedRawInput(t *testing.T) {
	raw := validSkill("go-testing", "description", strings.Repeat("x", MaxSkillBytes))
	if _, _, err := Parse("go-testing", raw); err == nil {
		t.Fatal("Parse() error = nil")
	}
}

func TestParseAcceptsExactRawSizeBoundary(t *testing.T) {
	prefix := validSkill("go-testing", "description", "")
	raw := append(prefix, []byte(strings.Repeat("x", MaxSkillBytes-len(prefix)))...)
	if _, content, err := Parse("go-testing", raw); err != nil || len(content) != MaxSkillBytes {
		t.Fatalf("Parse() content length/error = %d/%v", len(content), err)
	}
}
