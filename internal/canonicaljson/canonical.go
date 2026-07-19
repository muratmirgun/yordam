package canonicaljson

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/muratmirgun/yordam/internal/protocol"
)

type nodeKind uint8

const (
	nodeNull nodeKind = iota
	nodeBool
	nodeString
	nodeNumber
	nodeArray
	nodeObject
)

type node struct {
	kind    nodeKind
	boolean bool
	text    string
	array   []node
	object  map[string]node
}

func Marshal(value any) ([]byte, error) {
	var raw []byte
	var err error
	if message, ok := value.(json.RawMessage); ok {
		raw = bytes.Clone(message)
	} else {
		if err := validateGoValue(reflect.ValueOf(value), 0); err != nil {
			return nil, err
		}
		raw, err = json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal canonical JSON input: %w", err)
		}
	}
	root, err := parse(raw)
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := writeNode(&output, root); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func Digest(value any) (protocol.Digest, error) {
	raw, err := Marshal(value)
	if err != nil {
		return protocol.Digest{}, err
	}
	sum := sha256.Sum256(raw)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}, nil
}

func ValidateDigest(body any, got protocol.Digest) error {
	want, err := Digest(body)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("body digest mismatch: got %s:%s want %s:%s", got.Algorithm, got.Value, want.Algorithm, want.Value)
	}
	return nil
}

func TransactionDigest(events []protocol.EventEnvelope) (protocol.Digest, error) {
	hash := sha256.New()
	for _, event := range events {
		raw, err := Marshal(event)
		if err != nil {
			return protocol.Digest{}, err
		}
		_, _ = hash.Write(raw)
		_, _ = hash.Write([]byte{'\n'})
	}
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(hash.Sum(nil))}, nil
}

func parse(raw []byte) (node, error) {
	if len(raw) == 0 {
		return node{}, fmt.Errorf("canonical JSON input is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	root, err := parseValue(decoder, 0)
	if err != nil {
		return node{}, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return node{}, fmt.Errorf("trailing JSON token %v", token)
		}
		return node{}, fmt.Errorf("trailing JSON value: %w", err)
	}
	return root, nil
}

func parseValue(decoder *json.Decoder, depth int) (node, error) {
	token, err := decoder.Token()
	if err != nil {
		return node{}, fmt.Errorf("decode canonical JSON: %w", err)
	}
	switch value := token.(type) {
	case nil:
		return node{kind: nodeNull}, nil
	case bool:
		return node{kind: nodeBool, boolean: value}, nil
	case string:
		if len(value) > protocol.MaxStringBytes {
			return node{}, fmt.Errorf("string exceeds %d bytes", protocol.MaxStringBytes)
		}
		return node{kind: nodeString, text: value}, nil
	case json.Number:
		if !validInteger(value.String()) {
			return node{}, fmt.Errorf("non-integer JSON number %q", value)
		}
		return node{kind: nodeNumber, text: value.String()}, nil
	case json.Delim:
		if depth >= protocol.MaxJSONDepth {
			return node{}, fmt.Errorf("JSON depth exceeds %d", protocol.MaxJSONDepth)
		}
		switch value {
		case '[':
			items := make([]node, 0)
			for decoder.More() {
				if len(items) == protocol.MaxCollectionMembers {
					return node{}, fmt.Errorf("array exceeds %d members", protocol.MaxCollectionMembers)
				}
				item, err := parseValue(decoder, depth+1)
				if err != nil {
					return node{}, err
				}
				items = append(items, item)
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return node{}, fmt.Errorf("invalid array terminator")
			}
			return node{kind: nodeArray, array: items}, nil
		case '{':
			members := make(map[string]node)
			for decoder.More() {
				if len(members) == protocol.MaxCollectionMembers {
					return node{}, fmt.Errorf("object exceeds %d members", protocol.MaxCollectionMembers)
				}
				keyToken, err := decoder.Token()
				if err != nil {
					return node{}, fmt.Errorf("decode object key: %w", err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return node{}, fmt.Errorf("object key is not a string")
				}
				if len(key) > protocol.MaxStringBytes {
					return node{}, fmt.Errorf("object key exceeds %d bytes", protocol.MaxStringBytes)
				}
				if _, duplicate := members[key]; duplicate {
					return node{}, fmt.Errorf("duplicate object key %q", key)
				}
				member, err := parseValue(decoder, depth+1)
				if err != nil {
					return node{}, err
				}
				members[key] = member
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return node{}, fmt.Errorf("invalid object terminator")
			}
			return node{kind: nodeObject, object: members}, nil
		default:
			return node{}, fmt.Errorf("unexpected JSON delimiter %q", value)
		}
	default:
		return node{}, fmt.Errorf("unsupported JSON token %T", value)
	}
}

func writeNode(output *bytes.Buffer, value node) error {
	switch value.kind {
	case nodeNull:
		output.WriteString("null")
	case nodeBool:
		if value.boolean {
			output.WriteString("true")
		} else {
			output.WriteString("false")
		}
	case nodeString:
		raw, err := json.Marshal(value.text)
		if err != nil {
			return fmt.Errorf("encode canonical string: %w", err)
		}
		output.Write(raw)
	case nodeNumber:
		output.WriteString(value.text)
	case nodeArray:
		output.WriteByte('[')
		for index, item := range value.array {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := writeNode(output, item); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case nodeObject:
		keys := make([]string, 0, len(value.object))
		for key := range value.object {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			raw, err := json.Marshal(key)
			if err != nil {
				return fmt.Errorf("encode canonical object key: %w", err)
			}
			output.Write(raw)
			output.WriteByte(':')
			if err := writeNode(output, value.object[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("invalid canonical node kind %d", value.kind)
	}
	return nil
}

func validInteger(value string) bool {
	if value == "" {
		return false
	}
	start := 0
	if value[0] == '-' {
		if len(value) == 1 {
			return false
		}
		start = 1
	}
	if value[start] == '0' {
		return len(value) == start+1
	}
	if value[start] < '1' || value[start] > '9' {
		return false
	}
	for index := start + 1; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func validateGoValue(value reflect.Value, depth int) error {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		return validateGoValue(value.Elem(), depth)
	case reflect.String:
		if value.Len() > protocol.MaxStringBytes {
			return fmt.Errorf("string exceeds %d bytes", protocol.MaxStringBytes)
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 && value.Len() > protocol.MaxByteFieldBytes {
			return fmt.Errorf("byte field exceeds %d bytes", protocol.MaxByteFieldBytes)
		}
		if value.Len() > protocol.MaxCollectionMembers && value.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("slice exceeds %d members", protocol.MaxCollectionMembers)
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateGoValue(value.Index(index), depth+1); err != nil {
				return err
			}
		}
	case reflect.Array:
		if value.Len() > protocol.MaxCollectionMembers {
			return fmt.Errorf("array exceeds %d members", protocol.MaxCollectionMembers)
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateGoValue(value.Index(index), depth+1); err != nil {
				return err
			}
		}
	case reflect.Map:
		if value.Len() > protocol.MaxCollectionMembers {
			return fmt.Errorf("map exceeds %d members", protocol.MaxCollectionMembers)
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateGoValue(iterator.Key(), depth+1); err != nil {
				return err
			}
			if err := validateGoValue(iterator.Value(), depth+1); err != nil {
				return err
			}
		}
	case reflect.Struct:
		if depth >= protocol.MaxJSONDepth {
			return fmt.Errorf("value depth exceeds %d", protocol.MaxJSONDepth)
		}
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).PkgPath != "" {
				continue
			}
			if err := validateGoValue(value.Field(index), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
