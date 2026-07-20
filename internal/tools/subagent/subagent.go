// Package subagent defines the trusted descriptor for sequential child-turn
// orchestration. Ordinary tool execution is deliberately rejected: the
// orchestrator owns durable parent/child coordination.
package subagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	toolset "github.com/muratmirgun/yordam/internal/tools"
)

const Kind = "subagent"

var (
	inputSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"required":["task"],"properties":{"task":{"type":"string","minLength":1,"maxLength":32768},"expected_output":{"type":"string","maxLength":16384},"context":{"type":"string","maxLength":65536}}}`)

	ErrOrchestratorDispatchRequired = errors.New("subagent requires orchestrator dispatch")
)

// Tool has no execution dependencies because a prepared call is only a
// validated handoff. The orchestrator turns it into durable parent/child work.
type Tool struct{}

func New() *Tool { return &Tool{} }

func (*Tool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{
		Name:             Kind,
		Description:      "Delegate one bounded objective to a sequential child agent.",
		ScopeDescription: "Exact bounded child-task handoff.",
		InputSchema:      bytes.Clone(inputSchema),
		Mutation:         domain.MutationProcess,
	}
}

func (*Tool) CanonicalDescriptor() protocol.ToolDescriptor {
	return BuiltinDescriptor()
}

func (*Tool) TrustedClassification() domain.ToolClassification {
	return toolset.SubagentClassification()
}

func (*Tool) OrchestratedKind() string { return Kind }

func (t *Tool) Prepare(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	return t.prepare(ctx, request)
}

func (t *Tool) Plan(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	return t.prepare(ctx, request)
}

func (*Tool) prepare(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var call protocol.SubagentCallV1
	if err := decodeStrict(request.Input, &call); err != nil {
		return nil, fmt.Errorf("subagent input: %w", err)
	}
	if err := call.Validate(); err != nil {
		return nil, fmt.Errorf("subagent input: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &prepared{request: request, call: protocol.DeepCopy(call)}, nil
}

// BuiltinDescriptor is the only trusted descriptor accepted for orchestration
// interception and authorization.
func BuiltinDescriptor() protocol.ToolDescriptor {
	return toolset.BuiltinCanonicalDescriptor((&Tool{}).Descriptor(), toolset.SubagentClassification())
}

// IsCanonicalDescriptor requires every canonical field and digest to match;
// an alias alone can never make an ordinary tool orchestrated.
func IsCanonicalDescriptor(descriptor protocol.ToolDescriptor) bool {
	return reflect.DeepEqual(descriptor, BuiltinDescriptor())
}

type prepared struct {
	request domain.ToolRequest
	call    protocol.SubagentCallV1
}

func (p *prepared) Preview() domain.PreparedToolRequest {
	return domain.PreparedToolRequest{
		Request: p.request, Mutation: domain.MutationProcess, CanonicalScope: "subagent:" + p.request.CallID,
		InsideWorkspace: true, Summary: "delegate bounded child task",
	}
}

func (p *prepared) Revalidate(ctx context.Context) (domain.PreparedToolRequest, error) {
	if err := ctx.Err(); err != nil {
		return domain.PreparedToolRequest{}, err
	}
	return p.Preview(), nil
}

func (p *prepared) Execute(context.Context) domain.ToolResult {
	started := time.Now()
	return domain.ToolResult{CallID: p.request.CallID, Status: domain.ToolFailed, ErrorKind: domain.ErrorToolFailed, Content: ErrOrchestratorDispatchRequired.Error(), Duration: time.Since(started)}
}

func decodeStrict(raw json.RawMessage, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
