// Package skill exposes frozen skill-catalog entries as untrusted, bounded tool output.
package skill

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/muratmirgun/yordam/internal/secret"
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
	return t.prepare(ctx, request)
}

// Plan is intentionally the same frozen capture as Prepare: a skill is a
// logical catalog object, so planning must not defer loading to dispatch.
func (t *Tool) Plan(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	return t.prepare(ctx, request)
}

func (t *Tool) prepare(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
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
	if !ok || !validLoadedSkill(t.catalog.Snapshot(), parsed.Name, loaded) {
		return nil, fmt.Errorf("skill input: unavailable skill %q", parsed.Name)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &prepared{request: request, loaded: protocol.DeepCopy(loaded), output: t.output}, nil
}

func validLoadedSkill(snapshot protocol.SkillCatalogSnapshot, name string, loaded skills.LoadedSkill) bool {
	if snapshot.Validate() != nil || loaded.Identity.Validate() != nil || loaded.Identity.Name != name || loaded.Description == "" || len(loaded.Content) > skills.MaxSkillBytes {
		return false
	}
	metadata, normalized, err := skills.Parse(name, loaded.Content)
	if err != nil || metadata.Name != name || metadata.Description != loaded.Description || !bytes.Equal(normalized, loaded.Content) {
		return false
	}
	sum := sha256.Sum256(loaded.Content)
	if loaded.Identity.ContentDigest != (protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}) {
		return false
	}
	for _, active := range snapshot.Active {
		if active.Identity == loaded.Identity && active.Description == loaded.Description && active.State == protocol.SkillStateActive {
			return true
		}
	}
	return false
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
	redactor, closeRedactor, err := p.outputRedactor()
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("bind skill output redaction: %w", err))
	}
	defer closeRedactor()
	identity := redactedIdentity(protocol.DeepCopy(p.loaded.Identity), redactor)
	body, err := canonicaljson.Marshal(resultBody{Identity: identity, Description: redactor.String(p.loaded.Description), Source: protocol.SkillSource(redactor.String(string(identity.Source))), Digest: protocol.Digest{Algorithm: redactor.String(identity.ContentDigest.Algorithm), Value: redactor.String(identity.ContentDigest.Value)}, Provenance: provenance{CanonicalPath: redactor.String(identity.CanonicalPath), WorkspaceID: protocol.WorkspaceID(redactor.String(string(identity.WorkspaceID))), RuntimeGenerationID: protocol.RuntimeGenerationID(redactor.String(string(identity.RuntimeGenerationID)))}, Content: redactor.String(string(p.loaded.Content))})
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

func (p *prepared) outputRedactor() (secret.Redacting, func(), error) {
	if p.output.Admission != nil {
		lease, err := p.output.Admission.Derive()
		if err != nil {
			return nil, func() {}, err
		}
		return lease, func() { _ = lease.Close() }, nil
	}
	if p.output.Redact != nil {
		return p.output.Redact, func() {}, nil
	}
	return secret.New(), func() {}, nil
}

func redactedIdentity(identity protocol.SkillIdentity, redactor secret.Redacting) protocol.SkillIdentity {
	identity.Name = redactor.String(identity.Name)
	identity.Source = protocol.SkillSource(redactor.String(string(identity.Source)))
	identity.CanonicalPath = redactor.String(identity.CanonicalPath)
	identity.WorkspaceID = protocol.WorkspaceID(redactor.String(string(identity.WorkspaceID)))
	identity.ContentDigest.Algorithm = redactor.String(identity.ContentDigest.Algorithm)
	identity.ContentDigest.Value = redactor.String(identity.ContentDigest.Value)
	identity.RuntimeGenerationID = protocol.RuntimeGenerationID(redactor.String(string(identity.RuntimeGenerationID)))
	return identity
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
