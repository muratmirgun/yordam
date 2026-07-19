package skills

import (
	"bytes"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

func Parse(name string, raw []byte) (Metadata, []byte, error) {
	if len(raw) > MaxSkillBytes {
		return Metadata{}, nil, errors.New("skill exceeds maximum size")
	}
	if !validName(name) || !utf8.Valid(raw) || hasDisallowedByte(raw) {
		return Metadata{}, nil, errors.New("invalid skill input")
	}
	normalized := bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	if bytes.Contains(normalized, []byte("\r")) {
		return Metadata{}, nil, errors.New("invalid skill line ending")
	}
	if !bytes.HasPrefix(normalized, []byte("---\n")) {
		return Metadata{}, nil, errors.New("skill frontmatter must begin at byte zero")
	}
	fenceEnd := bytes.Index(normalized[4:], []byte("\n---\n"))
	if fenceEnd < 0 {
		return Metadata{}, nil, errors.New("skill frontmatter is unterminated")
	}
	fenceEnd += 4
	frontmatter := normalized[4:fenceEnd]
	body := normalized[fenceEnd+5:]
	if len(bytes.TrimSpace(body)) == 0 {
		return Metadata{}, nil, errors.New("skill body is empty")
	}
	metadata, err := parseFrontmatter(frontmatter)
	if err != nil || metadata.Name != name || !validName(metadata.Name) {
		return Metadata{}, nil, errors.New("invalid skill frontmatter")
	}
	return metadata, append([]byte(nil), normalized...), nil
}

func parseFrontmatter(raw []byte) (Metadata, error) {
	var metadata Metadata
	seenName, seenDescription := false, false
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(line) == 0 {
			return Metadata{}, errors.New("empty frontmatter line")
		}
		key, value, found := bytes.Cut(line, []byte(":"))
		if !found || !validScalar(string(value)) {
			return Metadata{}, errors.New("invalid frontmatter scalar")
		}
		valueString := string(value[1:])
		switch string(key) {
		case "name":
			if seenName {
				return Metadata{}, errors.New("duplicate name")
			}
			seenName, metadata.Name = true, valueString
		case "description":
			if seenDescription {
				return Metadata{}, errors.New("duplicate description")
			}
			if len(valueString) > 1024 || strings.TrimSpace(valueString) == "" {
				return Metadata{}, errors.New("invalid description")
			}
			seenDescription, metadata.Description = true, valueString
		default:
			return Metadata{}, errors.New("unknown frontmatter key")
		}
	}
	if !seenName || !seenDescription {
		return Metadata{}, errors.New("required frontmatter key missing")
	}
	return metadata, nil
}

func validScalar(value string) bool {
	if len(value) < 2 || value[0] != ' ' || value[1] == ' ' || value[len(value)-1] == ' ' || value[len(value)-1] == '\t' {
		return false
	}
	plain := value[1:]
	if strings.Contains(plain, "\\") || strings.Contains(plain, "#") || strings.Contains(plain, ": ") {
		return false
	}
	for index, character := range plain {
		if unicode.IsControl(character) {
			return false
		}
		if index == 0 && strings.ContainsRune("\"'&*!|>@[]{}`", character) {
			return false
		}
	}
	return true
}

func hasDisallowedByte(raw []byte) bool {
	for _, value := range raw {
		if value == 0 || (value < 0x20 && value != '\n' && value != '\r' && value != '\t') {
			return true
		}
	}
	return false
}

func validName(name string) bool {
	if name == "" || name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	previousHyphen := false
	for index := range name {
		character := name[index]
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			previousHyphen = false
			continue
		}
		if character == '-' && !previousHyphen {
			previousHyphen = true
			continue
		}
		return false
	}
	return true
}
