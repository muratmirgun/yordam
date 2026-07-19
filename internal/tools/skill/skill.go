// Package skill exposes frozen skill-catalog entries as untrusted, bounded tool output.
package skill

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/skills"
	toolset "github.com/muratmirgun/yordam/internal/tools"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

var inputSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string","pattern":"^[a-z0-9]+(?:-[a-z0-9]+)*$"}}}`)
var skillName = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

type input struct {
	Name string `json:"name"`
}

// Tool deliberately has no filesystem root or path. Catalog.Load is the sole
// capability boundary, and Prepare turns its copy into a frozen request value.
type Tool struct {
	catalog skills.Catalog
	output  output.Options
}

func New(catalog skills.Catalog, options output.Options) *Tool {
	if catalog == nil {
		panic("skill: catalog is required")
	}
	if options.Artifacts == nil {
		panic("skill: artifact store is required")
	}
	if err := catalog.Snapshot().Validate(); err != nil {
		panic(fmt.Sprintf("skill: invalid catalog: %v", err))
	}
	return &Tool{catalog: catalog, output: options}
}

func (*Tool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{Name: "skill", Description: "Load the complete content of a catalog-bound skill.", ScopeDescription: "Exact frozen skill identity.", InputSchema: bytes.Clone(inputSchema), Mutation: domain.MutationReadOnly}
}

func (t *Tool) CanonicalDescriptor() protocol.ToolDescriptor {
	return BuiltinDescriptor()
}

func (*Tool) TrustedClassification() domain.ToolClassification { return toolset.SkillClassification() }

// BuiltinDescriptor is the single trusted identity used by authorization. It
// intentionally contains no catalog data: individual capability binding lives
// in the frozen resource target prepared for each request.
func BuiltinDescriptor() protocol.ToolDescriptor {
	return toolset.BuiltinCanonicalDescriptor((&Tool{}).Descriptor(), toolset.SkillClassification())
}

func (t *Tool) Prepare(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var parsed input
	if err := decodeStrict(request.Input, &parsed); err != nil {
		return nil, fmt.Errorf("skill input: %w", err)
	}
	if !skillName.MatchString(parsed.Name) {
		return nil, fmt.Errorf("skill input: invalid name")
	}
	loaded, ok := t.catalog.Load(parsed.Name)
	if !ok || loaded.Identity.Validate() != nil || loaded.Identity.Name != parsed.Name || loaded.Description == "" {
		return nil, fmt.Errorf("skill input: unavailable skill %q", parsed.Name)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &prepared{request: request, loaded: protocol.DeepCopy(loaded), output: t.output}, nil
}

type prepared struct {
	request domain.ToolRequest
	loaded  skills.LoadedSkill
	output  output.Options
}

func (p *prepared) Preview() domain.PreparedToolRequest {
	identity := p.loaded.Identity
	digest := identity.ContentDigest.Algorithm + ":" + identity.ContentDigest.Value
	return domain.PreparedToolRequest{
		Request: p.request, Mutation: domain.MutationReadOnly, InsideWorkspace: true,
		CanonicalScope: "skill:" + identity.Name + ":" + string(identity.Source) + ":" + digest + ":" + string(identity.RuntimeGenerationID),
		Summary:        "load frozen skill " + identity.Name,
		Resources: []protocol.ResourceTarget{{Kind: "skill", CanonicalID: identity.Name, Digest: digest, Attributes: []protocol.ResourceAttribute{
			{Name: "runtime_generation", Value: string(identity.RuntimeGenerationID)},
			{Name: "source", Value: string(identity.Source)},
			{Name: "workspace_id", Value: string(identity.WorkspaceID)},
		}}},
	}
}

func (p *prepared) Revalidate(ctx context.Context) (domain.PreparedToolRequest, error) {
	if err := ctx.Err(); err != nil {
		return domain.PreparedToolRequest{}, err
	}
	return p.Preview(), nil
}

type resultBody struct {
	Identity    protocol.SkillIdentity `json:"identity"`
	Description string                 `json:"description"`
	Source      protocol.SkillSource   `json:"source"`
	Digest      protocol.Digest        `json:"digest"`
	Provenance  provenance             `json:"provenance"`
	Content     string                 `json:"content"`
}

type provenance struct {
	CanonicalPath       string                       `json:"canonical_path"`
	WorkspaceID         protocol.WorkspaceID         `json:"workspace_id"`
	RuntimeGenerationID protocol.RuntimeGenerationID `json:"runtime_generation_id"`
}

func (p *prepared) Execute(ctx context.Context) domain.ToolResult {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return cancelled(p.request.CallID, started, err)
	}
	identity := protocol.DeepCopy(p.loaded.Identity)
	body, err := canonicaljson.Marshal(resultBody{Identity: identity, Description: p.loaded.Description, Source: identity.Source, Digest: identity.ContentDigest, Provenance: provenance{CanonicalPath: identity.CanonicalPath, WorkspaceID: identity.WorkspaceID, RuntimeGenerationID: identity.RuntimeGenerationID}, Content: string(p.loaded.Content)})
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("encode skill output: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return cancelled(p.request.CallID, started, err)
	}
	buffer := output.New(p.output)
	defer buffer.Close()
	if _, err := buffer.Write(body); err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("write skill output: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return cancelled(p.request.CallID, started, err)
	}
	result, err := buffer.Result(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return cancelled(p.request.CallID, started, err)
		}
		return failed(p.request.CallID, started, fmt.Errorf("finalize skill output: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return cancelled(p.request.CallID, started, err)
	}
	result.CallID, result.Duration = p.request.CallID, time.Since(started)
	return result
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

func failed(callID string, started time.Time, err error) domain.ToolResult {
	return domain.ToolResult{CallID: callID, Status: domain.ToolFailed, ErrorKind: domain.ErrorToolFailed, Content: err.Error(), Duration: time.Since(started)}
}

func cancelled(callID string, started time.Time, err error) domain.ToolResult {
	return domain.ToolResult{CallID: callID, Status: domain.ToolCancelled, ErrorKind: domain.ErrorCancelled, Duration: time.Since(started)}
}
