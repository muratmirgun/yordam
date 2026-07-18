package agent

// Compaction is a serialized orchestrator control operation. The former
// provider-and-journal implementation was removed with the monolithic runner.
const MaxCompactionSummaryBytes = 128 << 10
