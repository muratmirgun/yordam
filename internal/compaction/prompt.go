package compaction

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	MaxSummaryBytes        = 128 << 10
	SummaryContractVersion = "compaction_summary_v1"
)

var summaryFields = map[string]struct{}{
	"goal": {}, "constraints": {}, "decisions": {}, "files": {}, "commands_and_tests": {}, "unresolved": {}, "children": {}, "skills": {}, "unknown_effects": {},
}

// BuildSummaryRequest produces a model-neutral structured-output request. The
// orchestrator supplies model/provider identity and authorizes egress later.
func BuildSummaryRequest(selection Selection, prior *protocol.ContentSource) (protocol.ModelRequest, error) {
	if err := validateSelection(selection); err != nil {
		return protocol.ModelRequest{}, err
	}
	priorText := "none"
	if prior != nil {
		if err := prior.Validate(); err != nil {
			return protocol.ModelRequest{}, fmt.Errorf("invalid prior compaction summary: %w", err)
		}
		raw, err := canonicaljson.Marshal(prior.Content)
		if err != nil {
			return protocol.ModelRequest{}, err
		}
		priorText = string(raw)
	}
	sources, err := canonicaljson.Marshal(selection.Sources)
	if err != nil {
		return protocol.ModelRequest{}, err
	}
	contract := `Return only canonical JSON with exactly these fields: {"goal":"","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}. Do not add keys or prose.`
	input := fmt.Sprintf("range_from=%s\nrange_through=%s\nsource_digest=%s:%s\nprior_summary=%s\nnormalized_sources=%s", cursorLabel(selection.From), cursorLabel(selection.Through), selection.SourceDigest.Algorithm, selection.SourceDigest.Value, priorText, sources)
	return protocol.ModelRequest{RequestID: "compaction-summary-v1", Messages: []protocol.ModelMessage{
		{Role: "system", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: contract}}},
		{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: input}}},
	}}, nil
}

// ParseSummary admits only the exact structured summary contract and returns its
// canonical JSON bytes for immutable evidence persistence.
func ParseSummary(raw []byte) ([]byte, error) {
	if len(raw) == 0 || len(raw) > MaxSummaryBytes || !utf8.Valid(raw) {
		return nil, fmt.Errorf("summary is empty, oversized, or not UTF-8")
	}
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&fields); err != nil {
		return nil, fmt.Errorf("decode summary: %w", err)
	}
	if len(fields) != len(summaryFields) {
		return nil, fmt.Errorf("summary must contain exactly the contract fields")
	}
	for name := range fields {
		if _, ok := summaryFields[name]; !ok {
			return nil, fmt.Errorf("unknown summary field %q", name)
		}
	}
	for name := range summaryFields {
		value, ok := fields[name]
		if !ok {
			return nil, fmt.Errorf("missing summary field %q", name)
		}
		if name == "goal" {
			var goal string
			if err := json.Unmarshal(value, &goal); err != nil {
				return nil, fmt.Errorf("summary goal must be a string")
			}
			continue
		}
		var list []json.RawMessage
		if err := json.Unmarshal(value, &list); err != nil || list == nil {
			return nil, fmt.Errorf("summary field %q must be an array", name)
		}
	}
	if decoder.More() {
		return nil, fmt.Errorf("trailing summary JSON")
	}
	canonical, err := canonicaljson.Marshal(json.RawMessage(raw))
	if err != nil {
		return nil, fmt.Errorf("canonicalize summary: %w", err)
	}
	if len(canonical) > MaxSummaryBytes {
		return nil, fmt.Errorf("canonical summary exceeds %d bytes", MaxSummaryBytes)
	}
	return canonical, nil
}

// Revision binds the admitted summary to the exact selected journal range. Trigger
// labels and clocks are deliberately absent from this canonical body.
func Revision(selection Selection, admitted []byte) (string, error) {
	if err := validateSelection(selection); err != nil {
		return "", err
	}
	canonical, err := ParseSummary(admitted)
	if err != nil {
		return "", err
	}
	summaryDigest, err := canonicaljson.Digest(json.RawMessage(canonical))
	if err != nil {
		return "", err
	}
	body := struct {
		ContractVersion string                   `json:"contract_version"`
		From            protocol.CommittedCursor `json:"from"`
		Through         protocol.CommittedCursor `json:"through"`
		SourceDigest    protocol.Digest          `json:"source_digest"`
		SummaryDigest   protocol.Digest          `json:"summary_digest"`
	}{SummaryContractVersion, selection.From, selection.Through, selection.SourceDigest, summaryDigest}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		return "", err
	}
	return strings.ToLower(digest.Value), nil
}

func validateSelection(selection Selection) error {
	if selection.From.Validate() != nil || selection.Through.Validate() != nil || selection.From.JournalKind != protocol.JournalSession || selection.From.JournalKind != selection.Through.JournalKind || selection.From.JournalID != selection.Through.JournalID || selection.From.CommitSeq > selection.Through.CommitSeq {
		return fmt.Errorf("invalid compaction selection range")
	}
	if len(selection.SummarizedEventIDs) == 0 || len(selection.Sources) == 0 || selection.SourceDigest.Validate() != nil {
		return fmt.Errorf("incomplete compaction selection")
	}
	digest, err := SourceDigest(selection.Sources)
	if err != nil {
		return err
	}
	if digest != selection.SourceDigest {
		return fmt.Errorf("compaction selection source digest mismatch")
	}
	return nil
}

func cursorLabel(cursor protocol.CommittedCursor) string {
	return fmt.Sprintf("%s/%s/%d/%s", cursor.JournalKind, cursor.JournalID, cursor.CommitSeq, cursor.TransactionID)
}
