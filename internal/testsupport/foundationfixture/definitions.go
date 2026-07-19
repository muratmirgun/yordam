package foundationfixture

type fileDefinition struct {
	path string
	data string
}

type definition struct {
	name  string
	files []fileDefinition
}

var definitions = []definition{
	{name: "evidence-digest-mismatch", files: []fileDefinition{
		{path: "input.json", data: `{"evidence_id":"evidence-corrupt","declared_digest":{"algorithm":"sha256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"actual_content":"digest mismatch fixture","producing_activity_id":"activity-evidence"}
`},
		{path: "want.json", data: `{"availability":"corrupt","verification":"invalid","diagnostics":["evidence blob digest mismatch"]}
`},
	}},
	{name: "incomplete-batch", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-incomplete","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"task.created","task_id":"task-incomplete","transaction_id":"txn-incomplete","payload":{"goal":"incomplete","outcome_contract_id":"contract-incomplete","contract_version":1}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":1}
`},
		{path: "want.json", data: `{"committed_events":1,"writable":false,"diagnostics":["incomplete_transaction"]}
`},
	}},
	{name: "invalid-known-payload", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":1,"event_id":"legacy-invalid-mode","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"mode.changed","payload":{}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":1}
`},
		{path: "want.json", data: `{"committed_events":1,"writable":false,"diagnostics":["invalid_known_payload"]}
`},
	}},
	{name: "invalid-sequence", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":1,"event_id":"legacy-gap","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:01:00Z","kind":"user.message","payload":{"content":"gap"}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":1}
`},
		{path: "want.json", data: `{"committed_events":1,"writable":false,"diagnostics":["invalid_sequence"]}
`},
	}},
	{name: "lineage-cycle", files: []fileDefinition{
		{path: "input.json", data: `{"sessions":[{"id":"session-parent","parent_id":"session-child","parent_cursor":{"commit_seq":2,"transaction_id":"txn-parent"}},{"id":"session-child","parent_id":"session-parent","parent_cursor":{"commit_seq":2,"transaction_id":"txn-child"}}]}
`},
		{path: "want.json", data: `{"accepted":false,"writable":false,"diagnostics":["lineage cycle"]}
`},
	}},
	{name: "missing-evidence-blob", files: []fileDefinition{
		{path: "input.json", data: `{"evidence_id":"evidence-missing","blob_digest":{"algorithm":"sha256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"blob_present":false,"producing_activity_id":"activity-evidence"}
`},
		{path: "want.json", data: `{"availability":"missing","verification":"invalid","diagnostics":["evidence blob is missing"]}
`},
	}},
	{name: "mixed-v1-v2", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":1,"event_id":"legacy-user-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"user.message","payload":{"content":"legacy"}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-mixed","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:02:00Z","kind":"migration.compatibility_declared","transaction_id":"txn-mixed","payload":{"reader_version":2,"writer_version":2,"legacy_head":{"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","commit_seq":2,"transaction_id":"legacy:legacy-user-1"},"downgrade_status":"v0.1_read_only_after_v2"}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-mixed-marker","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":4,"time":"2026-07-18T10:03:00Z","kind":"transaction.committed","transaction_id":"txn-mixed","payload":{"transaction_id":"txn-mixed","first_seq":3,"last_seq":3,"event_count":1,"digest":{"algorithm":"sha256","value":"51362cc57e520701da15e0e5c25390e7208ce1a1ce13aa1964362f03e9f16ab6"}}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:03:00Z","last_seq":4}
`},
		{path: "want.json", data: `{"committed_events":3,"writable":true,"diagnostics":[]}
`},
	}},
	{name: "unknown-future-kind", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-future-compat","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"migration.compatibility_declared","transaction_id":"txn-future","payload":{"reader_version":2,"writer_version":2,"legacy_head":{"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","commit_seq":1,"transaction_id":"legacy:legacy-session"},"downgrade_status":"v0.1_read_only_after_v2"}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-future","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:02:00Z","kind":"future.stateful","transaction_id":"txn-future","payload":{"future":true}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-future-marker","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":4,"time":"2026-07-18T10:03:00Z","kind":"transaction.committed","transaction_id":"txn-future","payload":{"transaction_id":"txn-future","first_seq":2,"last_seq":3,"event_count":2,"digest":{"algorithm":"sha256","value":"01cec305cb650864732ed8ad227f489bbed835bc65f61b931a81ee51067591c0"}}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:03:00Z","last_seq":4}
`},
		{path: "want.json", data: `{"committed_events":3,"writable":false,"diagnostics":["unsupported_event"]}
`},
	}},
	{name: "unsupported-envelope-version", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":3,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"future-envelope","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"future.event","transaction_id":"txn-envelope","payload":{}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":1}
`},
		{path: "want.json", data: `{"committed_events":1,"writable":false,"diagnostics":["unsupported_envelope_version"]}
`},
	}},
	{name: "unsupported-payload-version", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-unsupported-compat","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"migration.compatibility_declared","transaction_id":"txn-unsupported","payload":{"reader_version":2,"writer_version":2,"legacy_head":{"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","commit_seq":1,"transaction_id":"legacy:legacy-session"},"downgrade_status":"v0.1_read_only_after_v2"}}
{"schema_version":2,"payload_version":99,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-unsupported-payload","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:02:00Z","kind":"task.created","task_id":"task-future","transaction_id":"txn-unsupported","payload":{"goal":"future","outcome_contract_id":"contract-future","contract_version":1}}
{"schema_version":2,"payload_version":1,"journal_kind":"session","journal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_id":"v2-unsupported-marker","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":4,"time":"2026-07-18T10:03:00Z","kind":"transaction.committed","transaction_id":"txn-unsupported","payload":{"transaction_id":"txn-unsupported","first_seq":2,"last_seq":3,"event_count":2,"digest":{"algorithm":"sha256","value":"9f549f19e817d2a4050d5dbd2b3ab9ee5994946434dc523f5a29dc6435bbc639"}}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:03:00Z","last_seq":4}
`},
		{path: "want.json", data: `{"committed_events":3,"writable":false,"diagnostics":["unsupported_event"]}
`},
	}},
	{name: "v1-stale-edit-recovery", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":1,"event_id":"legacy-user","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"user.message","payload":{"content":"edit"}}
{"schema_version":1,"event_id":"legacy-plan","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:02:00Z","kind":"file.change_planned","payload":{"call_id":"stale-call","path":"${WORKSPACE}/README.md","expected_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","planned_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","diff":"legacy diff"}}
{"schema_version":1,"event_id":"legacy-start","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":4,"time":"2026-07-18T10:03:00Z","kind":"tool.started","payload":{"call_id":"stale-call"}}
{"schema_version":1,"event_id":"legacy-recovery","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":5,"time":"2026-07-18T10:04:00Z","kind":"turn.interrupted","payload":{"reason":"unmatched tool.started","call_count":1}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:04:00Z","last_seq":5}
`},
		{path: "want.json", data: `{"committed_events":5,"writable":true,"diagnostics":["migration.lossy"]}
`},
	}},
	{name: "v1-truncated-final", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":1,"event_id":"legacy-user-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"user.message","payload":{"content":"durable"}}
{"schema_version":1,"event_id":"legacy-truncated","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:02:00Z","kind":"assistant.message","payload":{"content":"partial`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:01:00Z","last_seq":2}
`},
		{path: "want.json", data: `{"committed_events":2,"writable":false,"diagnostics":["incomplete_final_fragment"]}
`},
	}},
	{name: "v1-unmatched-tool-start", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":1,"event_id":"legacy-user","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"user.message","payload":{"content":"mutate"}}
{"schema_version":1,"event_id":"legacy-request","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:02:00Z","kind":"tool.requested","payload":{"request":{"call_id":"open-call","name":"edit","input":{"path":"README.md"},"workspace":"${WORKSPACE}"},"mutation":"file","canonical_scope":"README.md","inside_workspace":true,"summary":"edit README"}}
{"schema_version":1,"event_id":"legacy-start","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":4,"time":"2026-07-18T10:03:00Z","kind":"tool.started","payload":{"call_id":"open-call"}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:03:00Z","last_seq":4}
`},
		{path: "want.json", data: `{"committed_events":4,"writable":true,"diagnostics":["migration.unmatched_activity"]}
`},
	}},
	{name: "v1-valid", files: []fileDefinition{
		{path: "events.jsonl", data: `{"schema_version":1,"event_id":"legacy-session","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":1,"time":"2026-07-18T10:00:00Z","kind":"session.created","payload":{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:00:00Z","last_seq":0}}
{"schema_version":1,"event_id":"legacy-user-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":2,"time":"2026-07-18T10:01:00Z","kind":"user.message","payload":{"content":"first turn"}}
{"schema_version":1,"event_id":"legacy-assistant-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":3,"time":"2026-07-18T10:02:00Z","kind":"assistant.message","payload":{"content":"working","tool_calls":[{"id":"same-call","name":"read","arguments":{"path":"README.md"}}]}}
{"schema_version":1,"event_id":"legacy-request-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":4,"time":"2026-07-18T10:03:00Z","kind":"tool.requested","payload":{"request":{"call_id":"same-call","name":"read","input":{"path":"README.md"},"workspace":"${WORKSPACE}"},"mutation":"read_only","canonical_scope":"README.md","inside_workspace":true,"summary":"read README"}}
{"schema_version":1,"event_id":"legacy-permission-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":5,"time":"2026-07-18T10:04:00Z","kind":"permission.resolved","payload":{"call_id":"same-call","tool":"read","decision":{"action":"allow","lifetime":"once","scope":"README.md","reason":"fixture"}}}
{"schema_version":1,"event_id":"legacy-start-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":6,"time":"2026-07-18T10:05:00Z","kind":"tool.started","payload":{"call_id":"same-call"}}
{"schema_version":1,"event_id":"legacy-result-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":7,"time":"2026-07-18T10:06:00Z","kind":"tool.result","payload":{"result":{"call_id":"same-call","status":"succeeded","content":"legacy output","artifact_ids":["legacy-artifact-1"],"duration":1000,"truncated":false}}}
{"schema_version":1,"event_id":"legacy-turn-1","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":8,"time":"2026-07-18T10:07:00Z","kind":"turn.completed","payload":{"reason":"model completed"}}
{"schema_version":1,"event_id":"legacy-user-2","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":9,"time":"2026-07-18T10:08:00Z","kind":"user.message","payload":{"content":"second turn"}}
{"schema_version":1,"event_id":"legacy-request-2","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":10,"time":"2026-07-18T10:09:00Z","kind":"tool.requested","payload":{"request":{"call_id":"same-call","name":"shell","input":{"command":"true"},"workspace":"${WORKSPACE}"},"mutation":"process","canonical_scope":"true","inside_workspace":true,"summary":"run command"}}
{"schema_version":1,"event_id":"legacy-start-2","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":11,"time":"2026-07-18T10:10:00Z","kind":"tool.started","payload":{"call_id":"same-call"}}
{"schema_version":1,"event_id":"legacy-result-2","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":12,"time":"2026-07-18T10:11:00Z","kind":"tool.result","payload":{"result":{"call_id":"same-call","status":"failed","error_kind":"tool_failed","content":"failed","duration":2000,"truncated":false}}}
{"schema_version":1,"event_id":"legacy-turn-2","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","seq":13,"time":"2026-07-18T10:12:00Z","kind":"turn.failed","payload":{"reason":"tool failed","error_kind":"tool_failed"}}
`},
		{path: "metadata.json", data: `{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":{"id":"${WORKSPACE_ID}","canonical_path":"${WORKSPACE}"},"title":"Fixture","mode":"ask","selection":{"profile":"default","model":"fixture-model"},"created_at":"2026-07-18T10:00:00Z","updated_at":"2026-07-18T10:12:00Z","last_seq":13}
`},
		{path: "want.json", data: `{"committed_events":13,"writable":true,"diagnostics":[]}
`},
	}},
}
