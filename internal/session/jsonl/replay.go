package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/safefile"
)

const (
	recoveryArtifactPrefix = "recovery-"
	recoveryArtifactSuffix = ".bin"
)

type recoveryGeneration struct {
	name string
	file *os.File
	info os.FileInfo
}

type recoveryTail struct {
	retained   []byte
	fullDigest string
}

type logCorruptionError struct {
	err error
}

func (e *logCorruptionError) Error() string { return e.err.Error() }
func (e *logCorruptionError) Unwrap() error { return e.err }

func corruptLogf(format string, args ...any) error {
	return &logCorruptionError{err: fmt.Errorf(format, args...)}
}

type missingEventLogError struct {
	sessionID string
}

func (e *missingEventLogError) Error() string {
	return fmt.Sprintf("storage corruption: session %q event log is missing", e.sessionID)
}

func (s *Store) Load(ctx context.Context, sessionID string) (domain.SessionReplay, error) {
	if err := s.state.lockContext(ctx); err != nil {
		return domain.SessionReplay{}, err
	}
	defer s.state.unlock()
	transaction, session, err := s.openSessionTransaction(ctx, sessionID, os.O_RDWR, 0)
	var missingLog *missingEventLogError
	if errors.As(err, &missingLog) {
		if session.LastSeq == 0 {
			return domain.SessionReplay{}, err
		}
		return domain.SessionReplay{
			Session:      session,
			RecoveryNote: missingLog.Error(),
			ReadOnly:     true,
		}, nil
	}
	if err != nil {
		return domain.SessionReplay{}, err
	}
	fail := func(operationErr error) (domain.SessionReplay, error) {
		return domain.SessionReplay{}, errors.Join(operationErr, transaction.close())
	}
	events, tail, validationErr := scanReplayLog(ctx, transaction.events, session.ID)
	if validationErr != nil {
		var corruption *logCorruptionError
		if !errors.As(validationErr, &corruption) {
			return fail(validationErr)
		}
		closeErr := transaction.close()
		if closeErr != nil {
			return domain.SessionReplay{}, closeErr
		}
		return domain.SessionReplay{
			Session:      session,
			Events:       events,
			RecoveryNote: fmt.Sprintf("corruption at sequence %d: %v", len(events)+1, validationErr),
			ReadOnly:     true,
		}, nil
	}
	validatedSeq := uint64(len(events))
	if session.LastSeq > validatedSeq {
		closeErr := transaction.close()
		if closeErr != nil {
			return domain.SessionReplay{}, closeErr
		}
		return domain.SessionReplay{
			Session:      session,
			Events:       events,
			RecoveryNote: fmt.Sprintf("storage corruption: metadata last sequence %d exceeds validated event log sequence %d", session.LastSeq, validatedSeq),
			ReadOnly:     true,
		}, nil
	}
	if validatedSeq == 0 {
		return fail(fmt.Errorf("storage corruption: published session %q has an empty event log", session.ID))
	}

	hasRecovery, err := durableRecoveryStatus(ctx, transaction)
	if err != nil {
		return fail(err)
	}
	note := ""
	if tail != nil {
		generation, err := ensureRecoveryGeneration(ctx, transaction, tail)
		if err != nil {
			return fail(err)
		}
		if err := ctx.Err(); err != nil {
			return fail(errors.Join(err, generation.close()))
		}
		if err := errors.Join(
			transaction.verifyArtifacts(),
			generation.verify(transaction),
		); err != nil {
			return fail(errors.Join(err, generation.close()))
		}
		truncateErr := transaction.events.Truncate(tail.offset)
		if truncateErr == nil {
			truncateErr = transaction.events.Sync()
		}
		if truncateErr == nil {
			truncateErr = errors.Join(transaction.verifyArtifacts(), generation.verify(transaction))
		}
		truncateErr = errors.Join(truncateErr, generation.close())
		if truncateErr != nil {
			return fail(truncateErr)
		}
		note = "recovered incomplete final line"
	} else if hasRecovery {
		note = "recovered incomplete final line on an earlier restart"
	}

	validatedTime := session.CreatedAt
	if len(events) > 0 {
		validatedTime = events[len(events)-1].Time
	}
	if session.LastSeq < validatedSeq {
		session.LastSeq = validatedSeq
		session.UpdatedAt = validatedTime
		if err := writeJSONAtomicRooted(ctx, transaction, session); err != nil {
			return fail(err)
		}
	}
	fileRecoveryNote, err := plannedFileRecoveryNote(ctx, events)
	if err != nil {
		return fail(err)
	}
	note = joinNote(note, fileRecoveryNote)
	openCallCount := unmatchedToolStarts(events)
	if openCallCount > 0 {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if err := transaction.verifyEvents(); err != nil {
			return fail(err)
		}
		event, err := s.appendInTransaction(ctx, transaction, session, domain.EventTurnInterrupted, map[string]any{
			"reason":     "unmatched tool.started",
			"call_count": openCallCount,
		})
		if err != nil {
			return fail(err)
		}
		events = append(events, event)
		session.LastSeq, session.UpdatedAt = event.Seq, event.Time
		note = joinNote(note, "marked unmatched tool call interrupted")
	}
	if err := transaction.close(); err != nil {
		return domain.SessionReplay{}, err
	}
	return domain.SessionReplay{Session: session, Events: events, RecoveryNote: note}, nil
}

type replayTail struct {
	recoveryTail
	offset int64
}

func scanReplayLog(ctx context.Context, file *os.File, sessionID string) ([]domain.DurableEvent, *replayTail, error) {
	if _, err := file.Seek(0, 0); err != nil {
		return nil, nil, err
	}
	reader := bufio.NewReaderSize(file, 64*1024)
	events := make([]domain.DurableEvent, 0)
	var line bytes.Buffer
	hasher := sha256.New()
	var completeOffset int64
	var lineBytes int64
	for {
		if err := ctx.Err(); err != nil {
			return events, nil, err
		}
		chunk, readErr := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			lineBytes += int64(len(chunk))
			_, _ = hasher.Write(chunk)
			if int64(line.Len()) < MaxArtifactBytes {
				remaining := int(MaxArtifactBytes - int64(line.Len()))
				line.Write(chunk[:min(len(chunk), remaining)])
			}
		}
		if errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return events, nil, err
		}
		if errors.Is(readErr, io.EOF) {
			if line.Len() == 0 {
				return events, nil, nil
			}
			return events, &replayTail{
				recoveryTail: recoveryTail{retained: append([]byte(nil), line.Bytes()...), fullDigest: hex.EncodeToString(hasher.Sum(nil))},
				offset:       completeOffset,
			}, nil
		}
		if readErr != nil {
			return events, nil, readErr
		}
		if lineBytes-1 > maxEventSize {
			return events, nil, corruptLogf("session event exceeds 2 MiB")
		}
		encoded := line.Bytes()[:line.Len()-1]
		index := len(events) + 1
		var event domain.DurableEvent
		if err := json.Unmarshal(encoded, &event); err != nil {
			return events, nil, corruptLogf("decode session event %d: %w", index, err)
		}
		if err := event.Validate(); err != nil {
			return events, nil, corruptLogf("validate session event %d: %w", index, err)
		}
		if event.SessionID != sessionID {
			return events, nil, corruptLogf("session event %d belongs to %q, want %q", index, event.SessionID, sessionID)
		}
		if event.Seq != uint64(index) {
			return events, nil, corruptLogf("session event sequence=%d want %d", event.Seq, index)
		}
		events = append(events, event)
		completeOffset += int64(line.Len())
		line.Reset()
		hasher.Reset()
		lineBytes = 0
	}
}

func durableRecoveryStatus(ctx context.Context, transaction *sessionTransaction) (bool, error) {
	if err := transaction.openArtifacts(); err != nil {
		return false, err
	}
	entries, err := transaction.artifactsRoot.Open(".")
	if err != nil {
		return false, err
	}
	directoryEntries, readErr := entries.ReadDir(-1)
	readErr = errors.Join(readErr, entries.Close())
	if readErr != nil {
		return false, readErr
	}
	hasRecovery := false
	for _, entry := range directoryEntries {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		digest, ok := recoveryDigest(entry.Name(), recoveryArtifactSuffix)
		if !ok {
			continue
		}
		valid, err := validateRecoveryLeaf(ctx, transaction, entry.Name(), digest)
		if err != nil {
			return false, err
		}
		hasRecovery = hasRecovery || valid
		temporary := "." + strings.TrimSuffix(entry.Name(), recoveryArtifactSuffix) + ".tmp"
		if _, err := transaction.artifactsRoot.Lstat(temporary); err == nil {
			if cleanupErr := reconcileRecoveryTemporary(ctx, transaction, temporary, digest, nil); cleanupErr != nil {
				return false, cleanupErr
			}
		} else if !os.IsNotExist(err) {
			return false, err
		}
	}
	return hasRecovery, transaction.verifyArtifacts()
}

func ensureRecoveryGeneration(ctx context.Context, transaction *sessionTransaction, tail *replayTail) (*recoveryGeneration, error) {
	if err := transaction.openArtifacts(); err != nil {
		return nil, err
	}
	retained := tail.retained
	fullDigest := tail.fullDigest
	retainedDigest := recoveryHash(retained)
	generationID := fullDigest + "-" + retainedDigest
	final := recoveryArtifactPrefix + generationID + recoveryArtifactSuffix
	temporary := "." + recoveryArtifactPrefix + generationID + ".tmp"

	finalFile, finalInfo, err := openRecoveryLeaf(ctx, transaction, final, retained)
	if err == nil {
		if _, temporaryErr := transaction.artifactsRoot.Lstat(temporary); temporaryErr == nil {
			if temporaryErr = reconcileRecoveryTemporary(ctx, transaction, temporary, retainedDigest, finalInfo); temporaryErr != nil {
				return nil, errors.Join(temporaryErr, finalFile.Close())
			}
		} else if !os.IsNotExist(temporaryErr) {
			return nil, errors.Join(temporaryErr, finalFile.Close())
		}
		return &recoveryGeneration{name: final, file: finalFile, info: finalInfo}, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	temporaryFile, temporaryInfo, err := openRootedRegularFile(ctx, transaction.artifactsRoot, temporary, os.O_RDONLY, 0)
	if err == nil {
		contents, readErr := readOpenedFile(ctx, temporaryFile, MaxArtifactBytes)
		if readErr == nil {
			readErr = errors.Join(
				transaction.verifyArtifacts(),
				verifyRootedRegularFile(transaction.artifactsRoot, temporary, temporaryInfo),
			)
		}
		if readErr != nil {
			return nil, errors.Join(readErr, temporaryFile.Close())
		}
		if !bytes.Equal(contents, retained) {
			cleanupErr := errors.Join(
				temporaryFile.Close(),
				cleanupRootEntryIfSame(transaction.artifactsRoot, temporaryInfo, temporary),
			)
			if cleanupErr != nil {
				return nil, cleanupErr
			}
			temporaryFile, temporaryInfo, err = nil, nil, os.ErrNotExist
		}
	}
	if os.IsNotExist(err) {
		temporaryFile, temporaryInfo, err = openRootedRegularFile(ctx, transaction.artifactsRoot, temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		writeErr := error(nil)
		if _, writeErr = temporaryFile.Write(retained); writeErr == nil {
			writeErr = temporaryFile.Sync()
		}
		if writeErr == nil {
			writeErr = verifyRootedRegularFile(transaction.artifactsRoot, temporary, temporaryInfo)
		}
		if writeErr != nil {
			return nil, errors.Join(writeErr, temporaryFile.Close(), cleanupRootEntryIfSame(transaction.artifactsRoot, temporaryInfo, temporary))
		}
	} else if err != nil {
		return nil, err
	} else if err := temporaryFile.Sync(); err != nil {
		return nil, errors.Join(err, temporaryFile.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, temporaryFile.Close())
	}
	if err := errors.Join(
		transaction.verifyArtifacts(),
		verifyRootedRegularFile(transaction.artifactsRoot, temporary, temporaryInfo),
	); err != nil {
		return nil, errors.Join(err, temporaryFile.Close())
	}
	if err := transaction.artifactsRoot.Link(temporary, final); err != nil {
		return nil, errors.Join(err, temporaryFile.Close())
	}
	if err := errors.Join(
		transaction.verifyArtifacts(),
		verifyRootedRegularFile(transaction.artifactsRoot, temporary, temporaryInfo),
		verifyRootedRegularFile(transaction.artifactsRoot, final, temporaryInfo),
	); err != nil {
		return nil, errors.Join(err, temporaryFile.Close())
	}
	if err := transaction.artifactsRoot.Remove(temporary); err != nil {
		return nil, errors.Join(err, temporaryFile.Close())
	}
	if err := syncRootDir(transaction.artifactsRoot, "."); err != nil {
		return nil, errors.Join(err, temporaryFile.Close())
	}
	return &recoveryGeneration{name: final, file: temporaryFile, info: temporaryInfo}, nil
}

func reconcileRecoveryTemporary(
	ctx context.Context,
	transaction *sessionTransaction,
	temporary string,
	digest string,
	finalInfo os.FileInfo,
) error {
	file, info, err := openRootedRegularFile(ctx, transaction.artifactsRoot, temporary, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	contents, readErr := readOpenedFile(ctx, file, MaxArtifactBytes)
	if readErr == nil && recoveryHash(contents) != digest {
		readErr = fmt.Errorf("recovery temporary %q does not match its digest", temporary)
	}
	if readErr == nil && finalInfo != nil && !os.SameFile(info, finalInfo) {
		readErr = fmt.Errorf("recovery temporary %q is not linked to its final generation", temporary)
	}
	if readErr == nil {
		readErr = errors.Join(
			transaction.verifyArtifacts(),
			verifyRootedRegularFile(transaction.artifactsRoot, temporary, info),
		)
	}
	readErr = errors.Join(readErr, file.Close())
	if readErr != nil {
		return readErr
	}
	if err := transaction.artifactsRoot.Remove(temporary); err != nil {
		return err
	}
	return syncRootDir(transaction.artifactsRoot, ".")
}

func openRecoveryLeaf(
	ctx context.Context,
	transaction *sessionTransaction,
	name string,
	want []byte,
) (*os.File, os.FileInfo, error) {
	file, info, err := openRootedRegularFile(ctx, transaction.artifactsRoot, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, err
	}
	contents, readErr := readOpenedFile(ctx, file, MaxArtifactBytes)
	if readErr == nil && !bytes.Equal(contents, want) {
		readErr = fmt.Errorf("recovery generation %q has unexpected contents", name)
	}
	if readErr == nil {
		readErr = errors.Join(
			transaction.verifyArtifacts(),
			verifyRootedRegularFile(transaction.artifactsRoot, name, info),
		)
	}
	if readErr != nil {
		return nil, nil, errors.Join(readErr, file.Close())
	}
	return file, info, nil
}

func validateRecoveryLeaf(ctx context.Context, transaction *sessionTransaction, name, digest string) (bool, error) {
	file, info, err := openRootedRegularFile(ctx, transaction.artifactsRoot, name, os.O_RDONLY, 0)
	if err != nil {
		return false, err
	}
	contents, readErr := readOpenedFile(ctx, file, MaxArtifactBytes)
	valid := readErr == nil && int64(len(contents)) <= MaxArtifactBytes && recoveryHash(contents) == digest
	if readErr == nil {
		readErr = errors.Join(
			transaction.verifyArtifacts(),
			verifyRootedRegularFile(transaction.artifactsRoot, name, info),
		)
	}
	return valid, errors.Join(readErr, file.Close())
}

func recoveryDigest(name, suffix string) (string, bool) {
	if !strings.HasPrefix(name, recoveryArtifactPrefix) || !strings.HasSuffix(name, suffix) {
		return "", false
	}
	generationID := strings.TrimSuffix(strings.TrimPrefix(name, recoveryArtifactPrefix), suffix)
	digests := strings.Split(generationID, "-")
	if len(digests) != 2 {
		return "", false
	}
	for _, digest := range digests {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
			return "", false
		}
	}
	return digests[1], true
}

func recoveryHash(contents []byte) string {
	digest := sha256.Sum256(contents)
	return hex.EncodeToString(digest[:])
}

func (g *recoveryGeneration) verify(transaction *sessionTransaction) error {
	return verifyRootedRegularFile(transaction.artifactsRoot, g.name, g.info)
}

func (g *recoveryGeneration) close() error {
	return g.file.Close()
}

func unmatchedToolStarts(events []domain.DurableEvent) int {
	openCalls := make(map[string]int)
	malformed := 0
	for _, event := range events {
		switch event.Kind {
		case domain.EventToolStarted:
			if callID, ok := eventCallID(event, false); ok {
				openCalls[callID]++
			} else {
				malformed++
			}
		case domain.EventToolResult:
			if callID, ok := eventCallID(event, true); ok && openCalls[callID] > 0 {
				openCalls[callID]--
			}
		case domain.EventTurnInterrupted:
			clear(openCalls)
			malformed = 0
		}
	}
	total := malformed
	for _, count := range openCalls {
		total += count
	}
	return total
}

func plannedFileRecoveryNote(ctx context.Context, events []domain.DurableEvent) (string, error) {
	pending := make(map[string]domain.FileChangePlan)
	for _, event := range events {
		switch event.Kind {
		case domain.EventFileChangePlanned:
			var plan domain.FileChangePlan
			if json.Unmarshal(event.Payload, &plan) == nil && plan.CallID != "" && plan.Path != "" {
				pending[plan.CallID] = plan
			}
		case domain.EventFileChanged:
			var change domain.FileChange
			if json.Unmarshal(event.Payload, &change) == nil && change.CallID != "" {
				delete(pending, change.CallID)
			}
		}
	}
	if len(pending) == 0 {
		return "", nil
	}

	callIDs := make([]string, 0, len(pending))
	for callID := range pending {
		callIDs = append(callIDs, callID)
	}
	sort.Strings(callIDs)
	note := ""
	for _, callID := range callIDs {
		matches, err := fileMatchesSHA256(ctx, pending[callID].Path, pending[callID].PlannedSHA256)
		if err != nil {
			return "", err
		}
		status := "planned change not confirmed"
		if matches {
			status = "planned after-state present"
		}
		note = joinNote(note, fmt.Sprintf("%s for call %s", status, callID))
	}
	return note, nil
}

func fileMatchesSHA256(ctx context.Context, path, want string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	file, err := safefile.OpenRegular(ctx, path)
	if err != nil {
		return false, nil
	}
	defer file.Close()

	hasher := sha256.New()
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		count, readErr := file.Read(buffer)
		if count > 0 {
			_, _ = hasher.Write(buffer[:count])
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return false, nil
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)) == strings.ToLower(want), nil
}

func eventCallID(event domain.DurableEvent, nestedResult bool) (string, bool) {
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return "", false
	}
	if callID, ok := nonEmptyString(payload["call_id"]); ok {
		return callID, true
	}
	if !nestedResult {
		return "", false
	}
	result, ok := payload["result"].(map[string]any)
	if !ok {
		return "", false
	}
	return nonEmptyString(result["call_id"])
}

func nonEmptyString(value any) (string, bool) {
	text, ok := value.(string)
	return text, ok && text != ""
}

func joinNote(left, right string) string {
	if left == "" {
		return right
	}
	return left + "; " + right
}
