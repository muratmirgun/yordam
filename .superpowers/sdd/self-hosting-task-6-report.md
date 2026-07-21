# Self-hosting Task 6 report

## Production audit

Audited the production config decoder and defaults, generated template, published schema, TUI command/help surfaces, permission policy, compaction policy, skill trust/catalog runtime, sequential-child coordinator, and recovery paths before changing product documentation.

No production behavior gap was found. The shipped surface includes `/compact`, `/skills`, model `contextWindow`, automatic and explicit compaction reserve policy, project ask/allow/deny trust with digest staleness and shadowing, reload-frozen runtime generations, depth-one same-provider/model sequential children, one active child, configured child limits, safe/ask/auto permission ceilings, trusted-shell acknowledgement, cancellation propagation, and fail-closed uncertain recovery without automatic retry.

## Documentation and configuration contract

- README now shows the complete v0.3 JSONC surface and records the exact defaults, bounds, commands, trust/reload behavior, recovery behavior, supported platforms, and explicit non-goals.
- SECURITY now states the permission-mode/trusted-shell boundary and exact non-authority guarantees for skills, children, compaction, and uncertain effects.
- CHANGELOG has an explicitly unpublished v0.3.0 implementation-candidate section; it does not claim a published release.
- The approved design is marked implemented on 2026-07-21, names its implementation plans and cumulative gate, and does not claim publication or release evidence results.
- The generated template now writes the safe explicit `projectPolicy: ask` value. Config tests prove that the configured template passes the strict runtime decoder with every v0.3 field.
- The schema retains the decoder's exact property sets and unknown-field rejection while documenting the shipped v0.3 settings and bounds.

## TDD and verification

```text
RED:
- generated template omitted the explicit projectPolicy field;
- README omitted the complete v0.3 config example and consolidated non-goals;
- SECURITY omitted consolidated exact capability boundaries;
- CHANGELOG omitted v0.3;
- the design retained implementation-planning status.

GREEN:
go test ./internal/config ./internal/repolint -run 'Test.*V030|Test.*Schema.*Template|TestGeneratedTemplateCoversV030StrictDecoderSurface' -count=1 -v
go test ./internal/config ./internal/repolint -count=1
go vet ./...
git diff --check
PASS

rg -n 'TODO|TBD|coming soon|not configured' README.md SECURITY.md CHANGELOG.md
rg -n -i 'planned for v0.3|will support parallel|supports parallel children|executes skill files|installs skill hooks|full access mode|automatically retries uncertain|yordam rewrites the source journal' README.md SECURITY.md CHANGELOG.md
No matches
```

No production code behavior was changed; `internal/config/save.go` changed only the first-run JSONC template bytes.
