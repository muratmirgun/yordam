package eventcodec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type Descriptor struct {
	Kind                  string
	Version               uint32
	New                   func() any
	ValidateStructural    func(any) error
	ValidateSemantic      func(any) error
	ValidateEnvelope      func(protocol.EventEnvelope, any) error
	AuthorizationCritical bool
	RedactionClass        string
	ProjectionDomains     []string
}

type registryKey struct {
	kind    string
	version uint32
}

type Registry struct {
	descriptors map[registryKey]Descriptor
	versions    map[string]map[uint32]struct{}
	mu          sync.RWMutex
}

type UnknownKindError struct {
	Kind string
}

func (e *UnknownKindError) Error() string {
	return fmt.Sprintf("unknown event kind %q", e.Kind)
}

type UnsupportedPayloadVersionError struct {
	Kind    string
	Version uint32
}

func (e *UnsupportedPayloadVersionError) Error() string {
	return fmt.Sprintf("unsupported payload version %d for event kind %q", e.Version, e.Kind)
}

func New(descriptors []Descriptor) (*Registry, error) {
	registry := &Registry{
		descriptors: make(map[registryKey]Descriptor, len(descriptors)),
		versions:    make(map[string]map[uint32]struct{}),
	}
	for _, descriptor := range descriptors {
		if descriptor.Kind == "" || descriptor.Version == 0 || descriptor.New == nil {
			return nil, fmt.Errorf("invalid event descriptor")
		}
		created := descriptor.New()
		if created == nil || reflect.TypeOf(created).Kind() != reflect.Pointer || reflect.ValueOf(created).IsNil() {
			return nil, fmt.Errorf("descriptor %s@%d New must return a nonnil pointer", descriptor.Kind, descriptor.Version)
		}
		key := registryKey{kind: descriptor.Kind, version: descriptor.Version}
		if _, exists := registry.descriptors[key]; exists {
			return nil, fmt.Errorf("duplicate event descriptor %s@%d", descriptor.Kind, descriptor.Version)
		}
		descriptor.ProjectionDomains = append([]string(nil), descriptor.ProjectionDomains...)
		registry.descriptors[key] = descriptor
		if registry.versions[descriptor.Kind] == nil {
			registry.versions[descriptor.Kind] = make(map[uint32]struct{})
		}
		registry.versions[descriptor.Kind][descriptor.Version] = struct{}{}
	}
	return registry, nil
}

func (r *Registry) Decode(rawEnvelope json.RawMessage) (protocol.EventRecord, error) {
	record := protocol.EventRecord{RawEnvelope: protocol.CloneRawMessage(rawEnvelope)}
	if len(rawEnvelope) == 0 || len(rawEnvelope) > protocol.MaxEventBytes {
		return record, fmt.Errorf("event envelope size must be between 1 and %d bytes", protocol.MaxEventBytes)
	}
	if _, err := canonicaljson.Marshal(rawEnvelope); err != nil {
		return record, fmt.Errorf("invalid event envelope JSON: %w", err)
	}
	if err := decodeStrict(rawEnvelope, &record.Envelope); err != nil {
		return record, fmt.Errorf("decode event envelope: %w", err)
	}
	if err := requireFields(rawEnvelope, reflect.TypeOf(protocol.EventEnvelope{})); err != nil {
		return record, fmt.Errorf("event envelope structure: %w", err)
	}
	if err := record.Envelope.ValidateEnvelope(); err != nil {
		return record, err
	}

	descriptor, err := r.lookup(record.Envelope.Kind, record.Envelope.PayloadVersion)
	if err != nil {
		return record, err
	}
	decoded := descriptor.New()
	if _, err := canonicaljson.Marshal(record.Envelope.Payload); err != nil {
		return record, fmt.Errorf("invalid payload JSON for %s@%d: %w", descriptor.Kind, descriptor.Version, err)
	}
	if err := decodeStrict(record.Envelope.Payload, decoded); err != nil {
		return record, fmt.Errorf("decode payload %s@%d: %w", descriptor.Kind, descriptor.Version, err)
	}
	if err := requireFields(record.Envelope.Payload, reflect.TypeOf(decoded).Elem()); err != nil {
		return record, fmt.Errorf("payload structure %s@%d: %w", descriptor.Kind, descriptor.Version, err)
	}
	record.Decoded = protocol.DeepCopy(decoded)
	if descriptor.ValidateEnvelope != nil {
		if err := descriptor.ValidateEnvelope(record.Envelope, record.Decoded); err != nil {
			return record, fmt.Errorf("event envelope %s@%d: %w", descriptor.Kind, descriptor.Version, err)
		}
	}
	if err := validateJournalFamily(record.Envelope, record.Decoded); err != nil {
		return record, err
	}
	if descriptor.ValidateStructural != nil {
		if err := descriptor.ValidateStructural(record.Decoded); err != nil {
			return record, fmt.Errorf("payload structure %s@%d: %w", descriptor.Kind, descriptor.Version, err)
		}
	}
	return protocol.CloneEventRecord(record), nil
}

func (r *Registry) Validate(record protocol.EventRecord) error {
	if err := record.Envelope.ValidateEnvelope(); err != nil {
		return err
	}
	descriptor, err := r.lookup(record.Envelope.Kind, record.Envelope.PayloadVersion)
	if err != nil {
		return err
	}
	if record.Decoded == nil {
		return fmt.Errorf("decoded payload is required")
	}
	expected := reflect.TypeOf(descriptor.New())
	if reflect.TypeOf(record.Decoded) != expected {
		return fmt.Errorf("decoded payload type %T, want %v", record.Decoded, expected)
	}
	fresh := descriptor.New()
	if err := decodeStrict(record.Envelope.Payload, fresh); err != nil {
		return fmt.Errorf("decode envelope payload for validation: %w", err)
	}
	if err := requireFields(record.Envelope.Payload, reflect.TypeOf(fresh).Elem()); err != nil {
		return fmt.Errorf("payload structure %s@%d: %w", descriptor.Kind, descriptor.Version, err)
	}
	if !reflect.DeepEqual(record.Decoded, fresh) {
		return fmt.Errorf("decoded payload does not match envelope payload")
	}
	if descriptor.ValidateEnvelope != nil {
		if err := descriptor.ValidateEnvelope(record.Envelope, record.Decoded); err != nil {
			return fmt.Errorf("event envelope %s@%d: %w", descriptor.Kind, descriptor.Version, err)
		}
	}
	if err := validateJournalFamily(record.Envelope, record.Decoded); err != nil {
		return err
	}
	if descriptor.ValidateStructural != nil {
		if err := descriptor.ValidateStructural(record.Decoded); err != nil {
			return fmt.Errorf("payload structure %s@%d: %w", descriptor.Kind, descriptor.Version, err)
		}
	}
	if descriptor.ValidateSemantic != nil {
		if err := descriptor.ValidateSemantic(record.Decoded); err != nil {
			return fmt.Errorf("payload semantics %s@%d: %w", descriptor.Kind, descriptor.Version, err)
		}
	}
	return validateEnvelopeIdentity(record.Envelope, record.Decoded)
}

func (r *Registry) Descriptor(kind string, version uint32) (Descriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	descriptor, ok := r.descriptors[registryKey{kind: kind, version: version}]
	if !ok {
		return Descriptor{}, false
	}
	descriptor.ProjectionDomains = append([]string(nil), descriptor.ProjectionDomains...)
	return descriptor, true
}

func (r *Registry) lookup(kind string, version uint32) (Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions, known := r.versions[kind]
	if !known {
		return Descriptor{}, &UnknownKindError{Kind: kind}
	}
	if _, supported := versions[version]; !supported {
		return Descriptor{}, &UnsupportedPayloadVersionError{Kind: kind, Version: version}
	}
	return r.descriptors[registryKey{kind: kind, version: version}], nil
}

func decodeStrict(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return fmt.Errorf("trailing JSON value: %w", err)
	}
	return nil
}

func requireFields(raw []byte, target reflect.Type) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	return requireValue(value, target, "$")
}

var (
	rawMessageType = reflect.TypeOf(json.RawMessage{})
	timeType       = reflect.TypeOf(time.Time{})
)

func requireValue(value any, target reflect.Type, path string) error {
	for target.Kind() == reflect.Pointer {
		if value == nil {
			return fmt.Errorf("%s must not be null", path)
		}
		target = target.Elem()
	}
	if target == rawMessageType {
		if value == nil {
			return fmt.Errorf("%s must not be null", path)
		}
		return nil
	}
	if target == timeType {
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a time string", path)
		}
		return nil
	}
	switch target.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		for index := 0; index < target.NumField(); index++ {
			field := target.Field(index)
			if field.PkgPath != "" {
				continue
			}
			tag := field.Tag.Get("json")
			name, options, _ := strings.Cut(tag, ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			fieldValue, exists := object[name]
			optional := false
			for _, option := range strings.Split(options, ",") {
				optional = optional || option == "omitempty"
			}
			if !exists {
				if !optional {
					return fmt.Errorf("%s.%s is required", path, name)
				}
				continue
			}
			if fieldValue == nil {
				return fmt.Errorf("%s.%s must not be null", path, name)
			}
			if err := requireValue(fieldValue, field.Type, path+"."+name); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if target == rawMessageType {
			return nil
		}
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s must be an array", path)
		}
		for index, item := range array {
			if err := requireValue(item, target.Elem(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("%s must be an object", path)
		}
	case reflect.Interface:
		if value == nil {
			return fmt.Errorf("%s must not be null", path)
		}
	case reflect.String:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
	case reflect.Bool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if _, ok := value.(json.Number); !ok {
			return fmt.Errorf("%s must be an integer", path)
		}
	}
	return nil
}
