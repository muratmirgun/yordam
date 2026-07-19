//go:build acceptance

package acceptance_test

import "testing"

func TestV020Foundation(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{"event_and_migration_matrix", acceptFoundationMigration},
		{"journal_concurrency", acceptFoundationConcurrency},
		{"durable_ordering", acceptFoundationOrdering},
		{"evidence_and_lineage", acceptFoundationEvidence},
		{"catalog_and_authorization", acceptFoundationCatalogAuthorization},
		{"application_protocol", acceptFoundationApplicationProtocol},
		{"orchestration_boundaries", acceptFoundationArchitecture},
		{"v010_regression", func(t *testing.T) { TestV010Acceptance(t) }},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}
