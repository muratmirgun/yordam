package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

const (
	EnvelopeVersion            uint32 = 2
	ApplicationProtocolVersion uint32 = 1

	MaxEventBytes        = 2 << 20
	MaxCommandBytes      = 2 << 20
	MaxJSONDepth         = 64
	MaxStringBytes       = 1 << 20
	MaxByteFieldBytes    = 1 << 20
	MaxCollectionMembers = 4096

	DigestSHA256 = "sha256"
)

type JournalKind string

const (
	JournalWorkspaceControl JournalKind = "workspace_control"
	JournalSession          JournalKind = "session"
)

type (
	JournalID           string
	SessionID           string
	TaskID              string
	OutcomeContractID   string
	TurnID              string
	ActivityID          string
	ActorID             string
	EventID             string
	EvidenceID          string
	CheckpointID        string
	ReceiptID           string
	ProviderID          string
	ModelID             string
	ToolID              string
	ToolAliasID         string
	RuntimeGenerationID string
	TransactionID       string
	CommandID           string
	ControlOperationID  string
	DecisionNonce       string
	WorkspaceID         string
	MCPServerID         string
	MCPItemID           string
	RecoveryMaterialID  string
)

type Digest struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

func (d Digest) Validate() error {
	if d.Algorithm != DigestSHA256 || len(d.Value) != 64 || strings.ToLower(d.Value) != d.Value {
		return fmt.Errorf("invalid digest")
	}
	if _, err := hex.DecodeString(d.Value); err != nil {
		return fmt.Errorf("invalid digest: %w", err)
	}
	return nil
}

func (d Digest) IsZero() bool {
	return d.Algorithm == "" && d.Value == ""
}

type JournalRef struct {
	Kind JournalKind `json:"journal_kind"`
	ID   JournalID   `json:"journal_id"`
}

func (r JournalRef) Validate() error {
	if !r.Kind.Valid() || r.ID == "" {
		return fmt.Errorf("invalid journal reference")
	}
	return nil
}

type CommittedCursor struct {
	JournalKind   JournalKind   `json:"journal_kind"`
	JournalID     JournalID     `json:"journal_id"`
	CommitSeq     uint64        `json:"commit_seq"`
	TransactionID TransactionID `json:"transaction_id"`
}

func (c CommittedCursor) Validate() error {
	if !c.JournalKind.Valid() || c.JournalID == "" || c.CommitSeq == 0 || c.TransactionID == "" {
		return fmt.Errorf("invalid committed cursor")
	}
	return nil
}

func (k JournalKind) Valid() bool {
	return k == JournalWorkspaceControl || k == JournalSession
}

type ActorKind string

const (
	ActorSystem   ActorKind = "system"
	ActorUser     ActorKind = "user"
	ActorAgent    ActorKind = "agent"
	ActorProvider ActorKind = "provider"
	ActorTool     ActorKind = "tool"
	ActorControl  ActorKind = "control"
)

type ActorRef struct {
	ID     ActorID   `json:"id"`
	Kind   ActorKind `json:"kind"`
	Source string    `json:"source,omitempty"`
}

func (a ActorRef) Validate() error {
	if a.ID == "" {
		return fmt.Errorf("actor ID is required")
	}
	switch a.Kind {
	case ActorSystem, ActorUser, ActorAgent, ActorProvider, ActorTool, ActorControl:
		return nil
	default:
		return fmt.Errorf("invalid actor kind %q", a.Kind)
	}
}

type ValueState string

const (
	ValueKnown       ValueState = "known"
	ValueUnknown     ValueState = "unknown"
	ValueUnavailable ValueState = "unavailable"
)

func (s ValueState) Valid() bool {
	return s == ValueKnown || s == ValueUnknown || s == ValueUnavailable
}

func ValidateRawJSON(raw json.RawMessage) error {
	return validateRawJSON(raw, MaxByteFieldBytes)
}

func validateRawJSON(raw json.RawMessage, maxBytes int) error {
	if len(raw) == 0 {
		return fmt.Errorf("JSON value is required")
	}
	if len(raw) > maxBytes {
		return fmt.Errorf("JSON value exceeds %d bytes", maxBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateJSONValue(decoder, 0); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON token %v", token)
		}
		return fmt.Errorf("trailing JSON value: %w", err)
	}
	return nil
}

func validateJSONValue(decoder *json.Decoder, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	switch value := token.(type) {
	case json.Delim:
		if depth >= MaxJSONDepth {
			return fmt.Errorf("JSON depth exceeds %d", MaxJSONDepth)
		}
		switch value {
		case '{':
			seen := make(map[string]struct{})
			count := 0
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("invalid object key: %w", err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("object key is not a string")
				}
				if len(key) > MaxStringBytes {
					return fmt.Errorf("object key exceeds %d bytes", MaxStringBytes)
				}
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("duplicate object key %q", key)
				}
				seen[key] = struct{}{}
				count++
				if count > MaxCollectionMembers {
					return fmt.Errorf("object exceeds %d members", MaxCollectionMembers)
				}
				if err := validateJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return fmt.Errorf("invalid object terminator")
			}
		case '[':
			count := 0
			for decoder.More() {
				count++
				if count > MaxCollectionMembers {
					return fmt.Errorf("array exceeds %d members", MaxCollectionMembers)
				}
				if err := validateJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return fmt.Errorf("invalid array terminator")
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", value)
		}
	case string:
		if len(value) > MaxStringBytes {
			return fmt.Errorf("string exceeds %d bytes", MaxStringBytes)
		}
	case json.Number:
		if !validInteger(value.String()) {
			return fmt.Errorf("non-integer JSON number %q", value)
		}
	case bool, nil:
		return nil
	default:
		return fmt.Errorf("unsupported JSON token %T", value)
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
	for i := start + 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

// DeepCopy returns a value whose mutable slices, maps, pointers, interfaces, and
// exported struct fields do not alias the input. Scalar and immutable values are
// copied directly.
func DeepCopy[T any](value T) T {
	copyValue := deepCopyValue(reflect.ValueOf(value))
	if !copyValue.IsValid() {
		var zero T
		return zero
	}
	return copyValue.Interface().(T)
}

func deepCopyValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copyValue := deepCopyValue(value.Elem())
		result := reflect.New(value.Type()).Elem()
		result.Set(copyValue)
		return result
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(deepCopyValue(value.Elem()))
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(deepCopyValue(value.Index(i)))
		}
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			result.SetMapIndex(deepCopyValue(iterator.Key()), deepCopyValue(iterator.Value()))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(deepCopyValue(value.Index(i)))
		}
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		// Copy the complete value first so immutable and unexported fields (for
		// example time.Time's representation) are preserved without reflecting
		// through inaccessible state. Exported fields are then recursively
		// replaced to detach every mutable wire value.
		result.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).PkgPath != "" {
				continue
			}
			result.Field(i).Set(deepCopyValue(value.Field(i)))
		}
		return result
	default:
		return value
	}
}

func CloneRawMessage(raw json.RawMessage) json.RawMessage {
	return DeepCopy(raw)
}

func ValidateBounds(value any) error {
	return validateBoundsValue(reflect.ValueOf(value), 0, make(map[visit]struct{}))
}

type visit struct {
	typeOf  reflect.Type
	pointer uintptr
}

func validateBoundsValue(value reflect.Value, depth int, seen map[visit]struct{}) error {
	if !value.IsValid() {
		return nil
	}
	if value.Type() == reflect.TypeOf(json.RawMessage{}) {
		if value.IsNil() {
			return nil
		}
		return ValidateRawJSON(value.Interface().(json.RawMessage))
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return validateBoundsValue(value.Elem(), depth, seen)
	case reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		entry := visit{typeOf: value.Type(), pointer: value.Pointer()}
		if _, exists := seen[entry]; exists {
			return nil
		}
		seen[entry] = struct{}{}
		defer delete(seen, entry)
		return validateBoundsValue(value.Elem(), depth, seen)
	case reflect.String:
		if value.Len() > MaxStringBytes {
			return fmt.Errorf("string exceeds %d bytes", MaxStringBytes)
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			if value.Len() > MaxByteFieldBytes {
				return fmt.Errorf("byte field exceeds %d bytes", MaxByteFieldBytes)
			}
			return nil
		}
		if depth >= MaxJSONDepth {
			return fmt.Errorf("value depth exceeds %d", MaxJSONDepth)
		}
		if value.Len() > MaxCollectionMembers {
			return fmt.Errorf("slice exceeds %d members", MaxCollectionMembers)
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateBoundsValue(value.Index(index), depth+1, seen); err != nil {
				return err
			}
		}
	case reflect.Array:
		if depth >= MaxJSONDepth {
			return fmt.Errorf("value depth exceeds %d", MaxJSONDepth)
		}
		if value.Len() > MaxCollectionMembers {
			return fmt.Errorf("array exceeds %d members", MaxCollectionMembers)
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateBoundsValue(value.Index(index), depth+1, seen); err != nil {
				return err
			}
		}
	case reflect.Map:
		if depth >= MaxJSONDepth {
			return fmt.Errorf("value depth exceeds %d", MaxJSONDepth)
		}
		if value.Len() > MaxCollectionMembers {
			return fmt.Errorf("map exceeds %d members", MaxCollectionMembers)
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateBoundsValue(iterator.Key(), depth+1, seen); err != nil {
				return err
			}
			if err := validateBoundsValue(iterator.Value(), depth+1, seen); err != nil {
				return err
			}
		}
	case reflect.Struct:
		if value.Type().PkgPath() == "time" && value.Type().Name() == "Time" {
			return nil
		}
		if depth >= MaxJSONDepth {
			return fmt.Errorf("value depth exceeds %d", MaxJSONDepth)
		}
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).PkgPath != "" {
				continue
			}
			if err := validateBoundsValue(value.Field(index), depth+1, seen); err != nil {
				return err
			}
		}
	}
	return nil
}
