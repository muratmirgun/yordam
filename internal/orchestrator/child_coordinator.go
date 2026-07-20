package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tooling"
)

// childTurnConfig is deliberately private. Only the canonical coordinator can
// mark a RunTurn request as a child, and it carries the frozen child exposure
// rather than a caller-supplied catalog revision.
type childTurnConfig struct {
	manifest protocol.SubagentManifestV1
	exposure protocol.ToolExposure
}

// SequentialChildCoordinator is the production implementation of the child
// handoff. It publishes only the already-reserved identity and delegates the
// actual turn to the ordinary Service.RunTurn lifecycle.
type SequentialChildCoordinator struct {
	sessions ChildSessionStore
	parents  ParentSessionInspector
	run      func(context.Context, StartTurnRequest) (RunResult, error)
}

func NewSequentialChildCoordinator(sessions ChildSessionStore, parents ParentSessionInspector, run func(context.Context, StartTurnRequest) (RunResult, error)) (*SequentialChildCoordinator, error) {
	if sessions == nil || parents == nil || run == nil {
		return nil, fmt.Errorf("child coordinator dependencies are incomplete")
	}
	return &SequentialChildCoordinator{sessions: sessions, parents: parents, run: run}, nil
}

func (c *SequentialChildCoordinator) RunChild(ctx context.Context, request ChildRunRequest) (protocol.SubagentReceiptV1, error) {
	if err := request.Manifest.Validate(); err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	if err := request.Call.Validate(); err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	if request.Parent.Runtime.ID != request.Manifest.RuntimeGenerationID || request.Parent.Runtime.Body.SkillCatalogRevision != request.Manifest.SkillCatalogRevision {
		return protocol.SubagentReceiptV1{}, fmt.Errorf("child manifest does not bind the frozen parent runtime")
	}
	parent, err := c.parents.InspectSession(ctx, request.Manifest.ParentSessionID)
	if err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	manifestDigest, err := canonicaljson.Digest(request.Manifest)
	if err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	lineage := &journal.SessionLineage{Kind: journal.LineageSubagent, ParentSessionID: request.Manifest.ParentSessionID, ParentCursor: request.Manifest.ParentCursor, DelegationAttemptID: request.Manifest.AttemptID, ManifestDigest: manifestDigest}
	if _, err := c.sessions.CreateWithIdentity(ctx, request.Manifest.ChildSessionID, parent.Session.Workspace, parent.Session.Mode, parent.Session.Selection, lineage); err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	childJournal, err := c.sessions.InspectSession(ctx, request.Manifest.ChildSessionID)
	if err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	exposure, err := derivedChildExposure(request.Parent.Runtime)
	if err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	digest, err := canonicaljson.Digest(struct {
		Manifest protocol.SubagentManifestV1 `json:"manifest"`
		Call     protocol.SubagentCallV1     `json:"call"`
	}{request.Manifest, request.Call})
	if err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	prompt := request.Call.Task
	if strings.TrimSpace(request.Call.ExpectedOutput) != "" {
		prompt += "\n\nExpected output:\n" + request.Call.ExpectedOutput
	}
	if strings.TrimSpace(request.Call.Context) != "" {
		prompt += "\n\nContext:\n" + request.Call.Context
	}
	childRequest := StartTurnRequest{
		Command:   CommandMetadata{CommandID: protocol.CommandID(stableID("subagent-child-command", string(request.Manifest.AttemptID))), IdempotencyKey: string(request.Manifest.AttemptID), RequestDigest: digest, Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}},
		SessionID: request.Manifest.ChildSessionID, ExpectedHead: childJournal.Head, Prompt: prompt,
		ProviderID: request.Parent.ProviderID, ModelID: request.Parent.ModelID, Runtime: protocol.DeepCopy(request.Parent.Runtime),
		child: &childTurnConfig{manifest: protocol.DeepCopy(request.Manifest), exposure: exposure},
	}
	_, runErr := c.run(ctx, childRequest)
	// RunTurn terminalizes ordinary failures durably. The receipt is always
	// loaded from the child journal so callers cannot fabricate a cursor.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	receipt, receiptErr := committedChildReceipt(cleanupCtx, c.sessions, request.Manifest)
	if receiptErr != nil {
		if runErr != nil {
			return protocol.SubagentReceiptV1{}, errors.Join(runErr, receiptErr)
		}
		return protocol.SubagentReceiptV1{}, receiptErr
	}
	return receipt, nil
}

func derivedChildExposure(runtime protocol.RuntimeGenerationManifest) (protocol.ToolExposure, error) {
	parent := toolExposure(runtime)
	tools := make([]protocol.ExposedTool, 0, len(parent.Tools))
	bindings := make([]protocol.ToolAliasBinding, 0, len(parent.Aliases))
	for index, descriptor := range runtime.Body.Tools {
		if canonicalSubagentDescriptor(descriptor) {
			continue
		}
		tools = append(tools, protocol.DeepCopy(parent.Tools[index]))
		bindings = append(bindings, protocol.DeepCopy(parent.Aliases[index]))
	}
	revision, err := tooling.DerivedExposureRevision(parent, tools, bindings)
	if err != nil {
		return protocol.ToolExposure{}, err
	}
	child := protocol.ToolExposure{CatalogRevision: revision, Tools: tools, Aliases: bindings}
	if err := tooling.ValidateDerivedExposure(parent, child); err != nil {
		return protocol.ToolExposure{}, err
	}
	return child, nil
}

func committedChildReceipt(ctx context.Context, sessions ChildSessionStore, manifest protocol.SubagentManifestV1) (protocol.SubagentReceiptV1, error) {
	inspection, err := sessions.InspectSession(ctx, manifest.ChildSessionID)
	if err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	for _, event := range inspection.Events {
		if event.Envelope.Kind != protocol.EventSubagentReceipt {
			continue
		}
		var receipt protocol.SubagentReceiptV1
		if err := json.Unmarshal(event.Envelope.Payload, &receipt); err != nil || receipt.Manifest != manifest || receipt.Validate() != nil {
			continue
		}
		cursor := protocol.CommittedCursor{JournalKind: event.Envelope.JournalKind, JournalID: event.Envelope.JournalID, CommitSeq: event.Envelope.Seq, TransactionID: event.Envelope.TransactionID}
		if receipt.TerminalCursor == cursor {
			return receipt, nil
		}
	}
	return protocol.SubagentReceiptV1{}, fmt.Errorf("child terminal receipt is absent")
}

var _ ChildCoordinator = (*SequentialChildCoordinator)(nil)
