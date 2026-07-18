package jsonl

type FaultPoint string

const (
	FaultEventWrite        FaultPoint = "event_write"
	FaultEventSync         FaultPoint = "event_sync"
	FaultMarkerWrite       FaultPoint = "marker_write"
	FaultMarkerSync        FaultPoint = "marker_sync"
	FaultMetadataWrite     FaultPoint = "metadata_write"
	FaultMetadataRename    FaultPoint = "metadata_rename"
	FaultDirectorySync     FaultPoint = "directory_sync"
	FaultCommittedViewSync FaultPoint = "committed_view_sync"

	FaultQuarantineWrite          FaultPoint = "quarantine_write"
	FaultQuarantineSync           FaultPoint = "quarantine_sync"
	FaultCandidateWrite           FaultPoint = "candidate_write"
	FaultCandidateSync            FaultPoint = "candidate_sync"
	FaultRecoveryMetadataWrite    FaultPoint = "recovery_metadata_write"
	FaultRecoveryMetadataSync     FaultPoint = "recovery_metadata_sync"
	FaultCandidateValidate        FaultPoint = "candidate_validate"
	FaultCandidateActivate        FaultPoint = "candidate_activate"
	FaultRecoveryDirectorySync    FaultPoint = "recovery_directory_sync"
	FaultRecoveryDiagnosticCommit FaultPoint = "recovery_diagnostic_commit"
)

type FaultInjector func(FaultPoint) error

func (s *Store) injectFault(point FaultPoint) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(point)
}
