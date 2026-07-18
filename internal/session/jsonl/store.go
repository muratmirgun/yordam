package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/oklog/ulid/v2"
)

type Options struct {
	Clock    func() time.Time
	Entropy  io.Reader
	Sanitize func(any) (json.RawMessage, error)
	Encoder  journal.Encoder
	Registry *eventcodec.Registry
	Fault    FaultInjector
}

type Store struct {
	root         string
	clock        func() time.Time
	entropy      io.Reader
	sanitize     func(any) (json.RawMessage, error)
	encoder      journal.Encoder
	registry     *eventcodec.Registry
	fault        FaultInjector
	state        *rootState
	verifiedScan sync.Map
}

type rootState struct {
	lock              chan struct{}
	idMu              sync.Mutex
	lastID            ulid.ULID
	hasLast           bool
	journalLocks      sync.Map
	markerUncertainty sync.Map
	lockIdentities    sync.Map
	activeCreates     sync.Map
}

var rootStates sync.Map

const maxEventSize = 2 << 20

func New(root string, opts Options) *Store {
	encoder := opts.Encoder
	if encoder == nil && opts.Sanitize != nil {
		encoder = sanitizeEncoder{sanitize: opts.Sanitize}
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.Entropy == nil {
		opts.Entropy = ulid.DefaultEntropy()
	}
	if opts.Sanitize == nil {
		opts.Sanitize = func(value any) (json.RawMessage, error) {
			return json.Marshal(value)
		}
	}
	registry := opts.Registry
	if registry == nil {
		registry, _ = eventcodec.New(eventcodec.FoundationDescriptors())
	}
	root = normalizeRoot(root)
	state, _ := rootStates.LoadOrStore(root, newRootState())
	return &Store{
		root: root, clock: opts.Clock, entropy: opts.Entropy, sanitize: opts.Sanitize,
		encoder: encoder, registry: registry, fault: opts.Fault, state: state.(*rootState),
	}
}

func newRootState() *rootState {
	state := &rootState{lock: make(chan struct{}, 1)}
	state.lock <- struct{}{}
	return state
}

func (s *rootState) lockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.lock:
		if err := ctx.Err(); err != nil {
			s.unlock()
			return err
		}
		return nil
	}
}

func (s *rootState) unlock() {
	s.lock <- struct{}{}
}

func normalizeRoot(root string) string {
	absolute, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return filepath.Clean(root)
	}
	cursor := absolute
	missing := []string{}
	for {
		if canonical, err := filepath.EvalSymlinks(cursor); err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				canonical = filepath.Join(canonical, missing[index])
			}
			return canonical
		}
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return absolute
		}
		missing = append(missing, filepath.Base(cursor))
		cursor = parent
	}
}

func (s *Store) nextID() (string, error) {
	s.state.idMu.Lock()
	defer s.state.idMu.Unlock()
	id, err := ulid.New(ulid.Timestamp(s.clock()), s.entropy)
	if err != nil {
		return "", err
	}
	if s.state.hasLast && id.Compare(s.state.lastID) <= 0 {
		id = s.state.lastID
		if !incrementULID(&id) {
			return "", fmt.Errorf("ULID space exhausted")
		}
	}
	s.state.lastID, s.state.hasLast = id, true
	return id.String(), nil
}

func incrementULID(id *ulid.ULID) bool {
	for index := len(id) - 1; index >= 0; index-- {
		id[index]++
		if id[index] != 0 {
			return true
		}
	}
	return false
}

func (s *Store) Create(ctx context.Context, workspace domain.Workspace, mode domain.PermissionMode, selection domain.ModelSelection) (domain.Session, error) {
	return s.create(ctx, workspace, mode, selection, nil)
}

func (s *Store) CreateWithLineage(ctx context.Context, workspace domain.Workspace, mode domain.PermissionMode, selection domain.ModelSelection, lineage *journal.SessionLineage) (domain.Session, error) {
	if lineage == nil {
		return s.create(ctx, workspace, mode, selection, nil)
	}
	copy := *lineage
	return s.create(ctx, workspace, mode, selection, &copy)
}

func (s *Store) create(ctx context.Context, workspace domain.Workspace, mode domain.PermissionMode, selection domain.ModelSelection, lineage *journal.SessionLineage) (domain.Session, error) {
	if err := validateWorkspace(workspace); err != nil {
		return domain.Session{}, err
	}
	if err := mode.Validate(); err != nil {
		return domain.Session{}, err
	}
	active := s.activeCreateCounter(workspace.ID)
	active.Add(1)
	defer active.Add(-1)
	var inheritedTurns []protocol.TurnID
	if lineage != nil {
		var err error
		inheritedTurns, err = s.validateLineageAnchor(ctx, workspace, *lineage)
		if err != nil {
			return domain.Session{}, err
		}
	}
	layout, err := s.openWorkspaceLayout(ctx, workspace, true)
	if err != nil {
		return domain.Session{}, err
	}
	defer func() { _ = layout.close() }()
	if err := reconcileSessionStaging(ctx, layout.sessionsRoot); err != nil {
		return domain.Session{}, err
	}
	id, err := s.nextID()
	if err != nil {
		return domain.Session{}, err
	}
	now := s.clock().UTC()
	session := domain.Session{
		ID:        id,
		Workspace: workspace,
		Title:     "New session",
		Mode:      mode,
		Selection: selection,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return s.createStagedSession(ctx, layout, session, lineage, inheritedTurns)
}

func (s *Store) activeCreateCounter(workspaceID string) *atomic.Int64 {
	loaded, _ := s.state.activeCreates.LoadOrStore(workspaceID, new(atomic.Int64))
	return loaded.(*atomic.Int64)
}

func (s *Store) createStagedSession(ctx context.Context, layout *workspaceLayout, session domain.Session, lineage *journal.SessionLineage, inheritedTurns []protocol.TurnID) (domain.Session, error) {
	stagingName, stagingRoot, stagingInfo, err := createSessionStaging(ctx, layout.sessionsRoot, session.ID)
	if err != nil {
		return domain.Session{}, err
	}
	published := false
	defer func() {
		_ = stagingRoot.Close()
		if !published {
			_ = layout.sessionsRoot.RemoveAll(stagingName)
			_ = syncRootDir(layout.sessionsRoot, ".")
		}
	}()
	if err := stagingRoot.Mkdir("artifacts", 0o700); err != nil {
		return domain.Session{}, err
	}
	events, updated, err := s.buildInitialJournal(session, lineage, inheritedTurns)
	if err != nil {
		return domain.Session{}, err
	}
	session = updated
	if err := writeStagedFile(ctx, stagingRoot, "events.jsonl", events); err != nil {
		return domain.Session{}, err
	}
	metadata, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return domain.Session{}, err
	}
	if int64(len(metadata)) > maxSessionMetadataBytes {
		return domain.Session{}, fmt.Errorf("session metadata exceeds %d bytes", maxSessionMetadataBytes)
	}
	if err := writeStagedFile(ctx, stagingRoot, "metadata.json", metadata); err != nil {
		return domain.Session{}, err
	}
	if err := writeStagedFile(ctx, stagingRoot, journalLockName, nil); err != nil {
		return domain.Session{}, err
	}
	if err := writeStagedFile(ctx, stagingRoot, turnLockName, nil); err != nil {
		return domain.Session{}, err
	}
	if lineage != nil {
		raw, err := canonicaljson.Marshal(*lineage)
		if err != nil {
			return domain.Session{}, err
		}
		if err := writeStagedFile(ctx, stagingRoot, lineageFileName, raw); err != nil {
			return domain.Session{}, err
		}
	}
	if err := syncRootDir(stagingRoot, "artifacts"); err != nil {
		return domain.Session{}, err
	}
	if err := syncRootDir(stagingRoot, "."); err != nil {
		return domain.Session{}, err
	}
	if err := errors.Join(
		layout.verify(),
		verifyRootedDirectory(layout.sessionsRoot, stagingName, stagingInfo),
		ctx.Err(),
	); err != nil {
		return domain.Session{}, err
	}
	if _, err := layout.sessionsRoot.Lstat(session.ID); err == nil {
		return domain.Session{}, fmt.Errorf("session %q already exists", session.ID)
	} else if !os.IsNotExist(err) {
		return domain.Session{}, err
	}
	if err := layout.sessionsRoot.Rename(stagingName, session.ID); err != nil {
		return domain.Session{}, err
	}
	published = true
	if err := syncRootDir(layout.sessionsRoot, "."); err != nil {
		return domain.Session{}, err
	}
	return session, nil
}

func (s *Store) buildInitialJournal(session domain.Session, lineage *journal.SessionLineage, inheritedTurns []protocol.TurnID) ([]byte, domain.Session, error) {
	eventTime := s.clock().UTC()
	if lineage == nil {
		createdPayload, err := json.Marshal(session)
		if err != nil {
			return nil, domain.Session{}, err
		}
		eventID, err := s.nextID()
		if err != nil {
			return nil, domain.Session{}, err
		}
		session.LastSeq, session.UpdatedAt = 1, eventTime
		event := domain.DurableEvent{SchemaVersion: 1, EventID: eventID, SessionID: session.ID, Seq: 1, Time: eventTime, Kind: domain.EventSessionCreated, Payload: createdPayload}
		if err := event.Validate(); err != nil {
			return nil, domain.Session{}, err
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, domain.Session{}, err
		}
		if len(encoded) > maxEventSize {
			return nil, domain.Session{}, fmt.Errorf("session event exceeds 2 MiB")
		}
		return append(encoded, '\n'), session, nil
	}
	transactionID, err := s.nextID()
	if err != nil {
		return nil, domain.Session{}, err
	}
	payloads := []struct {
		kind    string
		payload any
		turnID  protocol.TurnID
	}{
		{kind: protocol.EventSessionCreated, payload: protocol.SessionCreatedV1{WorkspaceID: protocol.WorkspaceID(session.Workspace.ID), CanonicalPath: session.Workspace.CanonicalPath, Title: session.Title, Mode: string(session.Mode), ProviderID: protocol.ProviderID(session.Selection.Profile), ModelID: protocol.ModelID(session.Selection.Model)}},
		{kind: protocol.EventSessionForked, payload: protocol.SessionForkedV1{ParentSessionID: lineage.ParentSessionID, ParentCursor: lineage.ParentCursor, CheckpointDigest: lineage.CheckpointDigest, TrustReset: true}},
	}
	for _, turnID := range inheritedTurns {
		payloads = append(payloads, struct {
			kind    string
			payload any
			turnID  protocol.TurnID
		}{kind: protocol.EventTurnInterrupted, turnID: turnID, payload: protocol.TurnTerminalV1{Status: "interrupted", Reason: "inherited historical interruption at fork"}})
	}
	payloads = append(payloads,
		struct {
			kind    string
			payload any
			turnID  protocol.TurnID
		}{kind: protocol.EventTrustedExecutionAcknowledged, payload: protocol.TrustedExecutionAcknowledgedV1{Enabled: false, Profile: "fork_reset"}},
		struct {
			kind    string
			payload any
			turnID  protocol.TurnID
		}{kind: protocol.EventSessionLifecycleChanged, payload: protocol.StateChangedV1{From: "forking", To: "idle", Reason: "lineage bootstrap complete"}},
	)
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	envelopes := make([]protocol.EventEnvelope, 0, len(payloads))
	journalBytes := make([]byte, 0)
	for index, item := range payloads {
		eventID, err := s.nextID()
		if err != nil {
			return nil, domain.Session{}, err
		}
		payload, err := canonicaljson.Marshal(item.payload)
		if err != nil {
			return nil, domain.Session{}, err
		}
		envelope := protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: ref.Kind, JournalID: ref.ID,
			EventID: protocol.EventID(eventID), SessionID: protocol.SessionID(session.ID), Seq: uint64(index + 1), Time: eventTime,
			Kind: item.kind, TurnID: item.turnID, TransactionID: protocol.TransactionID(transactionID), Payload: payload,
		}
		line, err := encodeLine(envelope)
		if err != nil {
			return nil, domain.Session{}, err
		}
		record, err := s.registry.Decode(line[:len(line)-1])
		if err != nil {
			return nil, domain.Session{}, err
		}
		if err := s.registry.Validate(record); err != nil {
			return nil, domain.Session{}, err
		}
		envelopes = append(envelopes, envelope)
		journalBytes = append(journalBytes, line...)
	}
	digest, err := canonicaljson.TransactionDigest(envelopes)
	if err != nil {
		return nil, domain.Session{}, err
	}
	markerID, err := s.nextID()
	if err != nil {
		return nil, domain.Session{}, err
	}
	markerPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{TransactionID: protocol.TransactionID(transactionID), FirstSeq: 1, LastSeq: uint64(len(envelopes)), EventCount: uint32(len(envelopes)), Digest: digest})
	if err != nil {
		return nil, domain.Session{}, err
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: ref.Kind, JournalID: ref.ID,
		EventID: protocol.EventID(markerID), SessionID: protocol.SessionID(session.ID), Seq: uint64(len(envelopes) + 1), Time: eventTime,
		Kind: protocol.EventTransactionCommitted, TransactionID: protocol.TransactionID(transactionID), Payload: markerPayload,
	}
	line, err := encodeLine(marker)
	if err != nil {
		return nil, domain.Session{}, err
	}
	journalBytes = append(journalBytes, line...)
	session.LastSeq, session.UpdatedAt = marker.Seq, eventTime
	return journalBytes, session, nil
}

func writeStagedFile(ctx context.Context, root *os.Root, name string, contents []byte) error {
	file, info, err := openRootedRegularFile(ctx, root, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := error(nil)
	if _, writeErr = file.Write(contents); writeErr == nil {
		writeErr = file.Sync()
	}
	if writeErr == nil {
		writeErr = verifyRootedRegularFile(root, name, info)
	}
	return errors.Join(writeErr, file.Close())
}

func (s *Store) Append(ctx context.Context, sessionID string, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
	if err := validateSessionID(sessionID); err != nil {
		return domain.DurableEvent{}, err
	}
	// Legacy mutation lock order is the existing root-wide v0.1 lock followed
	// by the session-keyed journal lock. No path acquires these in reverse.
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}
	lock := s.journalLock(ref)
	if err := lock.lock(ctx); err != nil {
		return domain.DurableEvent{}, err
	}
	defer lock.unlock()
	guard, err := s.acquireJournalMutation(ctx, ref)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	event, appendErr := s.appendLocked(ctx, domain.Session{ID: sessionID}, kind, payload, guard)
	return event, errors.Join(appendErr, guard.release())
}

func (s *Store) appendLocked(ctx context.Context, session domain.Session, kind domain.EventKind, payload any, guard *journalMutationGuard) (domain.DurableEvent, error) {
	transaction, durableSession, err := s.openSessionTransaction(ctx, session.ID, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	if err := guard.bind(ctx, transaction); err != nil {
		return domain.DurableEvent{}, errors.Join(err, transaction.close())
	}
	event, operationErr := s.appendInTransaction(ctx, transaction, durableSession, kind, payload)
	return event, errors.Join(operationErr, transaction.close())
}

func (s *Store) appendInTransaction(
	ctx context.Context,
	transaction *sessionTransaction,
	session domain.Session,
	kind domain.EventKind,
	payload any,
) (domain.DurableEvent, error) {
	raw, err := s.sanitize(payload)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	id, err := s.nextID()
	if err != nil {
		return domain.DurableEvent{}, err
	}
	event := domain.DurableEvent{
		SchemaVersion: 1,
		EventID:       id,
		SessionID:     session.ID,
		Seq:           session.LastSeq + 1,
		Time:          s.clock().UTC(),
		Kind:          kind,
		Payload:       raw,
	}
	if err := event.Validate(); err != nil {
		return domain.DurableEvent{}, err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	if len(encoded) > maxEventSize {
		return domain.DurableEvent{}, fmt.Errorf("session event exceeds 2 MiB")
	}
	// Phase 1 deliberately validates the full log before every append. This is
	// O(n) per append, but preserves fail-closed corruption detection without a
	// crash-sensitive side index; revisit after v0.1.
	summary, err := validateEventLog(ctx, transaction.events, session.ID, true)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	durableSeq := summary.lastSeq
	if durableSeq != session.LastSeq {
		return domain.DurableEvent{}, fmt.Errorf("session sequence mismatch: metadata=%d log=%d", session.LastSeq, durableSeq)
	}
	if err := transaction.verifyEvents(); err != nil {
		return domain.DurableEvent{}, err
	}
	_, err = transaction.events.Write(append(encoded, '\n'))
	if err == nil {
		err = transaction.events.Sync()
	}
	if err == nil {
		err = transaction.verifyEvents()
	}
	if err != nil {
		return domain.DurableEvent{}, err
	}
	session.LastSeq, session.UpdatedAt = event.Seq, event.Time
	if err := writeJSONAtomicRooted(ctx, transaction, session); err != nil {
		return domain.DurableEvent{}, err
	}
	return event, nil
}

type eventLogSummary struct {
	count   uint64
	lastSeq uint64
}

func validateCompleteLog(raw []byte, sessionID string) (eventLogSummary, error) {
	return validateEventLog(context.Background(), bytes.NewReader(raw), sessionID, true)
}

func validateEventLog(ctx context.Context, reader io.ReadSeeker, sessionID string, requireComplete bool) (eventLogSummary, error) {
	var summary eventLogSummary
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return summary, err
	}
	buffered := bufio.NewReaderSize(reader, 64*1024)
	var pending bytes.Buffer
	for {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		line, readErr := buffered.ReadSlice('\n')
		if pending.Len()+len(line) > maxEventSize+1 {
			return summary, fmt.Errorf("session event exceeds 2 MiB")
		}
		pending.Write(line)
		if errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			if pending.Len() == 0 {
				return summary, nil
			}
			if requireComplete {
				return summary, fmt.Errorf("session log has incomplete tail")
			}
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return summary, readErr
		}
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		line = pending.Bytes()
		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}
		index := summary.count + 1
		var event domain.DurableEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return summary, fmt.Errorf("decode session event %d: %w", index, err)
		}
		if err := event.Validate(); err != nil {
			return summary, fmt.Errorf("validate session event %d: %w", index, err)
		}
		if event.SessionID != sessionID {
			return summary, fmt.Errorf("session event %d belongs to %q, want %q", index, event.SessionID, sessionID)
		}
		if event.Seq != index {
			return summary, fmt.Errorf("session event sequence=%d want %d", event.Seq, index)
		}
		summary.count = index
		summary.lastSeq = event.Seq
		pending.Reset()
		if errors.Is(readErr, io.EOF) {
			return summary, nil
		}
	}
}

func validateSessionID(id string) error {
	parsed, err := ulid.ParseStrict(id)
	if err != nil || parsed.String() != id {
		return fmt.Errorf("invalid session ID %q", id)
	}
	return nil
}

func (s *Store) List(ctx context.Context, workspace domain.Workspace) ([]domain.SessionSummary, error) {
	if err := validateWorkspace(workspace); err != nil {
		return nil, err
	}
	for s.activeCreateCounter(workspace.ID).Load() > 0 {
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	layout, err := s.openWorkspaceLayout(ctx, workspace, false)
	if errors.Is(err, errWorkspaceNotFound) {
		return []domain.SessionSummary{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = layout.close() }()
	directory, err := layout.sessionsRoot.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := directory.ReadDir(-1)
	readErr = errors.Join(readErr, directory.Close())
	if readErr != nil {
		return nil, readErr
	}
	out := make([]domain.SessionSummary, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if validSessionStagingName(entry.Name()) {
			continue
		}
		if validateSessionID(entry.Name()) != nil {
			continue
		}
		if !entry.IsDir() {
			return nil, fmt.Errorf("session entry %q is not a directory", entry.Name())
		}
		sessionRoot, sessionInfo, err := openRootedDirectory(layout.sessionsRoot, entry.Name())
		if err != nil {
			return nil, err
		}
		metadata, metadataInfo, err := openRootedRegularFile(ctx, sessionRoot, "metadata.json", os.O_RDONLY, 0)
		if err != nil {
			sessionRoot.Close()
			return nil, err
		}
		raw, readErr := readOpenedFile(ctx, metadata, maxSessionMetadataBytes)
		if readErr == nil {
			readErr = errors.Join(
				layout.verify(),
				verifyRootedDirectory(layout.sessionsRoot, entry.Name(), sessionInfo),
				verifyRootedRegularFile(sessionRoot, "metadata.json", metadataInfo),
			)
		}
		readErr = errors.Join(readErr, metadata.Close(), sessionRoot.Close())
		if readErr != nil {
			return nil, readErr
		}
		var session domain.Session
		if err := json.Unmarshal(raw, &session); err != nil {
			return nil, err
		}
		if session.ID != entry.Name() || session.Workspace.ID != workspace.ID || session.Workspace.CanonicalPath != workspace.CanonicalPath {
			return nil, fmt.Errorf("session metadata identity mismatch for %q", entry.Name())
		}
		out = append(out, domain.SessionSummary{
			ID:        session.ID,
			Title:     session.Title,
			Mode:      session.Mode,
			UpdatedAt: session.UpdatedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}
