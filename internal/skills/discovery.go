package skills

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/safefile"
)

// discoveryHook exists solely to make identity substitution boundaries testable.
// Tests serialize access; the nil production path has no observable behavior.
var discoveryHook func(stage, name string)

type retainedRoot struct {
	path  string
	guard *os.File
	root  *os.Root
	info  os.FileInfo
}

type scannedCandidate struct {
	candidate Candidate
	name      string
	leaf      os.FileInfo
	rejected  bool
}

func Discover(ctx context.Context, options DiscoveryOptions) (Discovery, error) {
	if err := validateOptions(options); err != nil {
		return Discovery{}, err
	}
	if err := ctx.Err(); err != nil {
		return Discovery{}, err
	}
	result := Discovery{}
	for _, sourceRoot := range []struct {
		source protocol.SkillSource
		path   string
	}{
		{protocol.SkillSourceGlobal, options.GlobalRoot},
		{protocol.SkillSourceProject, options.ProjectRoot},
	} {
		candidates, diagnostics, err := discoverRoot(ctx, sourceRoot.source, sourceRoot.path, options)
		if err != nil {
			return Discovery{}, err
		}
		result.Candidates = append(result.Candidates, candidates...)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
	}
	if err := ctx.Err(); err != nil {
		return Discovery{}, err
	}
	sort.Slice(result.Candidates, func(left, right int) bool {
		return candidateSortKey(result.Candidates[left]) < candidateSortKey(result.Candidates[right])
	})
	result.Diagnostics = sortedDiagnostics(result.Diagnostics)
	return result, nil
}

func validateOptions(options DiscoveryOptions) error {
	if options.GenerationID == "" || options.Workspace.ID == "" || !absoluteClean(options.Workspace.CanonicalPath) {
		return errors.New("invalid skill discovery options")
	}
	workspaceInfo, err := os.Lstat(options.Workspace.CanonicalPath)
	if err != nil || !workspaceInfo.IsDir() || workspaceInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid canonical workspace")
	}
	canonicalWorkspace, err := filepath.EvalSymlinks(options.Workspace.CanonicalPath)
	if err != nil || canonicalWorkspace != options.Workspace.CanonicalPath {
		return errors.New("invalid canonical workspace")
	}
	if options.GlobalRoot != "" && !absoluteClean(options.GlobalRoot) || !absoluteClean(options.ProjectRoot) || options.ProjectRoot != filepath.Join(options.Workspace.CanonicalPath, ".yordam", "skills") {
		return errors.New("invalid skill discovery root")
	}
	return nil
}

func absoluteClean(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path
}

func discoverRoot(ctx context.Context, source protocol.SkillSource, path string, options DiscoveryOptions) ([]Candidate, []protocol.Diagnostic, error) {
	if path == "" {
		return nil, nil, nil
	}
	root, missing, err := openRetainedRoot(ctx, path)
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if missing {
		return nil, nil, nil
	}
	if err != nil {
		return nil, []protocol.Diagnostic{skillDiagnostic(source, "root_rejected", path, "")}, nil
	}
	defer root.root.Close()
	defer root.guard.Close()
	entries, err := rootedEntries(ctx, root.root)
	if err != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return nil, nil, cancellation
		}
		return nil, []protocol.Diagnostic{skillDiagnostic(source, "root_rejected", path, "")}, nil
	}
	caseCounts := make(map[string]int, len(entries))
	for _, entry := range entries {
		caseCounts[strings.ToLower(entry.Name())]++
	}
	scanned := make([]scannedCandidate, 0, len(entries))
	diagnostics := make([]protocol.Diagnostic, 0)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		name := entry.Name()
		if !validName(name) {
			diagnostics = append(diagnostics, skillDiagnostic(source, "entry_rejected", root.path, name))
			continue
		}
		if caseCounts[strings.ToLower(name)] != 1 {
			diagnostics = append(diagnostics, skillDiagnostic(source, "ambiguous_name", root.path, name))
			continue
		}
		record, diagnostic, scanErr := scanEntry(ctx, root, source, name, options)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		if cancellation := ctx.Err(); cancellation != nil {
			return nil, nil, cancellation
		}
		record.name = name
		if diagnostic != nil {
			diagnostics = append(diagnostics, *diagnostic)
			if record.leaf != nil {
				scanned = append(scanned, record)
			}
			continue
		}
		scanned = append(scanned, record)
	}
	for index := range scanned {
		for other := range scanned {
			if index != other && os.SameFile(scanned[index].leaf, scanned[other].leaf) {
				scanned[index].rejected = true
			}
		}
		if scanned[index].rejected && scanned[index].leaf != nil {
			for other := range scanned {
				if index != other && os.SameFile(scanned[index].leaf, scanned[other].leaf) {
					diagnostics = append(diagnostics, skillDiagnostic(source, "ambiguous_identity", root.path, scanned[index].name))
					break
				}
			}
		}
	}
	if err := verifyRetainedRoot(ctx, root); err != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return nil, nil, cancellation
		}
		return nil, []protocol.Diagnostic{skillDiagnostic(source, "root_rejected", path, "")}, nil
	}
	candidates := make([]Candidate, 0, len(scanned))
	for _, record := range scanned {
		if !record.rejected {
			candidates = append(candidates, record.candidate)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return candidates, diagnostics, nil
}

func openRetainedRoot(ctx context.Context, path string) (retainedRoot, bool, error) {
	if err := ctx.Err(); err != nil {
		return retainedRoot{}, false, err
	}
	state := rootPathState(path)
	if state == rootPathMissing {
		return retainedRoot{}, true, nil
	}
	if state != rootPathSafe {
		return retainedRoot{}, false, errors.New("unsafe skill root")
	}
	guard, err := safefile.OpenDirectory(ctx, path)
	if err != nil {
		return retainedRoot{}, false, err
	}
	guardInfo, err := guard.Stat()
	if err != nil || !guardInfo.IsDir() {
		_ = guard.Close()
		return retainedRoot{}, false, errors.New("unsafe skill root")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		_ = guard.Close()
		return retainedRoot{}, false, err
	}
	retained := retainedRoot{path: path, guard: guard, root: root, info: guardInfo}
	if discoveryHook != nil {
		discoveryHook("after-root-open", "")
	}
	if err := ctx.Err(); err != nil {
		_ = root.Close()
		_ = guard.Close()
		return retainedRoot{}, false, err
	}
	if err := verifyRetainedRoot(ctx, retained); err != nil {
		_ = root.Close()
		_ = guard.Close()
		return retainedRoot{}, false, err
	}
	return retained, false, nil
}

type rootPathStatus uint8

const (
	rootPathUnsafe rootPathStatus = iota
	rootPathMissing
	rootPathSafe
)

func rootPathState(path string) rootPathStatus {
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return rootPathMissing
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return rootPathUnsafe
		}
	}
	return rootPathSafe
}

func verifyRetainedRoot(ctx context.Context, root retainedRoot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rootPathState(root.path) != rootPathSafe {
		return errors.New("skill root path changed")
	}
	freshGuard, err := safefile.OpenDirectory(ctx, root.path)
	if err != nil {
		return err
	}
	freshInfo, statErr := freshGuard.Stat()
	closeErr := freshGuard.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(root.info, freshInfo) {
		return errors.New("skill root guard changed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	checked, err := os.Lstat(root.path)
	if err != nil || !checked.IsDir() || checked.Mode()&os.ModeSymlink != 0 || !os.SameFile(root.info, checked) {
		return errors.New("skill root changed")
	}
	guardInfo, err := root.guard.Stat()
	if err != nil || !os.SameFile(root.info, guardInfo) {
		return errors.New("retained root guard changed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	opened, err := root.root.Open(".")
	if err != nil {
		return err
	}
	openedInfo, statErr := opened.Stat()
	closeErr = opened.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(root.info, openedInfo) {
		return errors.New("retained root changed")
	}
	return nil
}

func rootedEntries(ctx context.Context, root *os.Root) ([]os.DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	return entries, nil
}

func scanEntry(ctx context.Context, root retainedRoot, source protocol.SkillSource, name string, options DiscoveryOptions) (scannedCandidate, *protocol.Diagnostic, error) {
	checkedDirectory, err := root.root.Lstat(name)
	if err != nil || !checkedDirectory.IsDir() || checkedDirectory.Mode()&os.ModeSymlink != 0 {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "entry_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	if discoveryHook != nil {
		discoveryHook("after-directory-inspect", name)
	}
	if err := ctx.Err(); err != nil {
		return scannedCandidate{}, nil, err
	}
	directory, err := root.root.OpenRoot(name)
	if err != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "entry_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	defer directory.Close()
	openedDirectory, err := directory.Open(".")
	if err != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "entry_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	openedDirectoryInfo, statErr := openedDirectory.Stat()
	closeErr := openedDirectory.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(checkedDirectory, openedDirectoryInfo) || verifyDirectory(root.root, name, checkedDirectory) != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "entry_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	if err := verifyRetainedRoot(ctx, root); err != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "root_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	checkedFile, err := directory.Lstat("SKILL.md")
	if err != nil || !checkedFile.Mode().IsRegular() || checkedFile.Mode()&os.ModeSymlink != 0 {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "file_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	if discoveryHook != nil {
		discoveryHook("after-file-inspect", name)
	}
	if err := ctx.Err(); err != nil {
		return scannedCandidate{}, nil, err
	}
	file, err := openRootedRegularNoFollow(directory, "SKILL.md", checkedFile)
	if err != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "file_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	defer file.Close()
	openedFile, err := file.Stat()
	if err != nil || !openedFile.Mode().IsRegular() || !os.SameFile(checkedFile, openedFile) || verifyFile(directory, "SKILL.md", checkedFile) != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "file_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	if discoveryHook != nil {
		discoveryHook("after-file-open", name)
	}
	if err := ctx.Err(); err != nil {
		return scannedCandidate{}, nil, err
	}
	raw, err := readBounded(ctx, file)
	if err != nil || verifyFile(directory, "SKILL.md", checkedFile) != nil || verifyDirectory(root.root, name, checkedDirectory) != nil || verifyRetainedRoot(ctx, root) != nil {
		if cancellation := ctx.Err(); cancellation != nil {
			return scannedCandidate{}, nil, cancellation
		}
		diagnostic := skillDiagnostic(source, "file_rejected", root.path, name)
		return scannedCandidate{}, &diagnostic, nil
	}
	metadata, content, err := Parse(name, raw)
	if err != nil {
		diagnostic := skillDiagnostic(source, "parse_rejected", root.path, name)
		return scannedCandidate{leaf: openedFile, rejected: true}, &diagnostic, nil
	}
	sum := sha256.Sum256(content)
	candidate := Candidate{Name: metadata.Name, Description: metadata.Description, Content: append([]byte(nil), content...), Source: source, CanonicalPath: filepath.Join(root.path, name, "SKILL.md"), ContentDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: fmt.Sprintf("%x", sum)}}
	if source == protocol.SkillSourceProject {
		candidate.WorkspaceID = protocol.WorkspaceID(options.Workspace.ID)
	}
	return scannedCandidate{candidate: candidate, leaf: openedFile}, nil, nil
}

func verifyDirectory(parent *os.Root, name string, expected os.FileInfo) error {
	checked, err := parent.Lstat(name)
	if err != nil || !checked.IsDir() || checked.Mode()&os.ModeSymlink != 0 || !os.SameFile(expected, checked) {
		return errors.New("directory changed")
	}
	return nil
}

func verifyFile(parent *os.Root, name string, expected os.FileInfo) error {
	checked, err := parent.Lstat(name)
	if err != nil || !checked.Mode().IsRegular() || checked.Mode()&os.ModeSymlink != 0 || !os.SameFile(expected, checked) {
		return errors.New("file changed")
	}
	return nil
}

func readBounded(ctx context.Context, file *os.File) ([]byte, error) {
	reader := io.LimitReader(file, MaxSkillBytes+1)
	contents := make([]byte, 0, 4096)
	buffer := make([]byte, 32<<10)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count, err := reader.Read(buffer)
		if count > 0 {
			contents = append(contents, buffer[:count]...)
			if len(contents) > MaxSkillBytes {
				return nil, errors.New("skill exceeds maximum size")
			}
		}
		if err == io.EOF {
			return contents, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func skillDiagnostic(source protocol.SkillSource, reason, rootPath, name string) protocol.Diagnostic {
	details, _ := json.Marshal(struct {
		Source string `json:"source"`
		Name   string `json:"name,omitempty"`
		Path   string `json:"path"`
		Reason string `json:"reason"`
	}{Source: string(source), Name: name, Path: filepath.Join(rootPath, name), Reason: reason})
	return protocol.Diagnostic{Code: "skills." + string(source) + "." + reason, Message: "skill discovery rejected untrusted filesystem input", Details: details}
}

func candidateSortKey(candidate Candidate) string {
	return string(candidate.Source) + "\x00" + candidate.Name + "\x00" + candidate.ContentDigest.Algorithm + "\x00" + candidate.ContentDigest.Value
}

func sortedDiagnostics(diagnostics []protocol.Diagnostic) []protocol.Diagnostic {
	sort.Slice(diagnostics, func(left, right int) bool {
		return diagnosticKey(diagnostics[left]) < diagnosticKey(diagnostics[right])
	})
	if len(diagnostics) == 0 {
		return diagnostics
	}
	unique := diagnostics[:0]
	previous := ""
	for _, diagnostic := range diagnostics {
		key := diagnosticKey(diagnostic)
		if key != previous {
			unique = append(unique, diagnostic)
			previous = key
		}
	}
	return unique
}

func diagnosticKey(diagnostic protocol.Diagnostic) string {
	return diagnostic.Code + "\x00" + diagnostic.Message + "\x00" + string(diagnostic.Journal.Kind) + "\x00" + string(diagnostic.Journal.ID) + "\x00" + string(diagnostic.EventID) + "\x00" + string(diagnostic.Details)
}
