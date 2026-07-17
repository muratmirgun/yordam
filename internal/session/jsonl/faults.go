package jsonl

type FaultPoint string

const (
	FaultEventWrite     FaultPoint = "event_write"
	FaultEventSync      FaultPoint = "event_sync"
	FaultMarkerWrite    FaultPoint = "marker_write"
	FaultMarkerSync     FaultPoint = "marker_sync"
	FaultMetadataWrite  FaultPoint = "metadata_write"
	FaultMetadataRename FaultPoint = "metadata_rename"
	FaultDirectorySync  FaultPoint = "directory_sync"
)

type FaultInjector func(FaultPoint) error

func (s *Store) injectFault(point FaultPoint) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(point)
}
