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
	"time"

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
	if err := s.state.lockContext(ctx); err != nil {
		return domain.Session{}, err
	}
	defer s.state.unlock()
	if err := validateWorkspace(workspace); err != nil {
		return domain.Session{}, err
	}
	if err := mode.Validate(); err != nil {
		return domain.Session{}, err
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
	return s.createStagedSession(ctx, layout, session)
}

func (s *Store) createStagedSession(ctx context.Context, layout *workspaceLayout, session domain.Session) (domain.Session, error) {
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
	createdPayload, err := json.Marshal(session)
	if err != nil {
		return domain.Session{}, err
	}
	eventID, err := s.nextID()
	if err != nil {
		return domain.Session{}, err
	}
	eventTime := s.clock().UTC()
	session.LastSeq, session.UpdatedAt = 1, eventTime
	event := domain.DurableEvent{
		SchemaVersion: 1,
		EventID:       eventID,
		SessionID:     session.ID,
		Seq:           1,
		Time:          eventTime,
		Kind:          domain.EventSessionCreated,
		Payload:       createdPayload,
	}
	if err := event.Validate(); err != nil {
		return domain.Session{}, err
	}
	encodedEvent, err := json.Marshal(event)
	if err != nil {
		return domain.Session{}, err
	}
	if len(encodedEvent) > maxEventSize {
		return domain.Session{}, fmt.Errorf("session event exceeds 2 MiB")
	}
	if err := writeStagedFile(ctx, stagingRoot, "events.jsonl", append(encodedEvent, '\n')); err != nil {
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
	if err := s.state.lockContext(ctx); err != nil {
		return domain.DurableEvent{}, err
	}
	defer s.state.unlock()
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
	return s.appendLocked(ctx, domain.Session{ID: sessionID}, kind, payload)
}

func (s *Store) appendLocked(ctx context.Context, session domain.Session, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
	transaction, durableSession, err := s.openSessionTransaction(ctx, session.ID, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return domain.DurableEvent{}, err
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
	if err := s.state.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.state.unlock()
	if err := validateWorkspace(workspace); err != nil {
		return nil, err
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
