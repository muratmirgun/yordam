package ptyfixture

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// CleanHead returns the exact checked-out commit only when root is a clean Git
// worktree, including its untracked files. Release acceptance must never build
// from bytes that are absent from that commit.
func CleanHead(root string) (string, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", errors.New("release input root must be an absolute clean path")
	}
	head, err := gitOutput(root, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve release input HEAD: %w", err)
	}
	status, err := gitOutput(root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return "", fmt.Errorf("inspect release input: %w", err)
	}
	if status != "" {
		return "", fmt.Errorf("release input is dirty")
	}
	if len(head) != 40 && len(head) != 64 {
		return "", fmt.Errorf("release input HEAD has invalid object ID")
	}
	for _, value := range head {
		if value < '0' || value > '9' && value < 'a' || value > 'f' {
			return "", fmt.Errorf("release input HEAD has invalid object ID")
		}
	}
	return head, nil
}

// CloneExact makes a detached local clone without hardlinks and verifies that
// it contains exactly the requested clean commit.
func CloneExact(source, destination, commit string) error {
	clean, err := CleanHead(source)
	if err != nil {
		return err
	}
	if clean != commit {
		return fmt.Errorf("release input HEAD %s does not match requested commit %s", clean, commit)
	}
	if destination == "" || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return errors.New("clone destination must be an absolute clean path")
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		if err == nil {
			return fmt.Errorf("clone destination already exists")
		}
		return fmt.Errorf("inspect clone destination: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return fmt.Errorf("create clone parent: %w", err)
	}
	if _, err := commandOutput("", "git", "clone", "--quiet", "--local", "--no-hardlinks", "--no-checkout", source, destination); err != nil {
		return fmt.Errorf("clone exact release input: %w", err)
	}
	if _, err := commandOutput("", "git", "-c", "advice.detachedHead=false", "-C", destination, "checkout", "--quiet", "--detach", commit); err != nil {
		return fmt.Errorf("checkout exact release commit: %w", err)
	}
	cloned, err := CleanHead(destination)
	if err != nil {
		return fmt.Errorf("verify exact clone: %w", err)
	}
	if cloned != commit {
		return fmt.Errorf("exact clone resolved %s, want %s", cloned, commit)
	}
	return nil
}

// BuildExact builds one package with release identity bound through the same
// buildinfo variables used by GoReleaser. Arguments are passed without a shell.
func BuildExact(source, binary, version, commit, date, packagePath string) error {
	head, err := CleanHead(source)
	if err != nil {
		return err
	}
	if head != commit {
		return fmt.Errorf("release input HEAD %s does not match build commit %s", head, commit)
	}
	if binary == "" || !filepath.IsAbs(binary) || filepath.Clean(binary) != binary {
		return errors.New("binary path must be an absolute clean path")
	}
	for name, value := range map[string]string{"version": version, "commit": commit, "date": date, "package": packagePath} {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("invalid %s build identity", name)
		}
	}
	module, err := modulePath(filepath.Join(source, "go.mod"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(binary), 0o700); err != nil {
		return fmt.Errorf("create binary directory: %w", err)
	}
	prefix := module + "/internal/buildinfo."
	ldflags := "-X " + prefix + "Version=" + version + " -X " + prefix + "Commit=" + commit + " -X " + prefix + "Date=" + date
	if _, err := commandOutput(source, "go", "build", "-trimpath", "-ldflags", ldflags, "-o", binary, packagePath); err != nil {
		return fmt.Errorf("build exact release commit: %w", err)
	}
	return nil
}

// TreeDigest hashes all worktree paths and bytes, including untracked files,
// while excluding Git's private bookkeeping directory.
func TreeDigest(root string) (string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative == ".git" {
			return filepath.SkipDir
		}
		paths = append(paths, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	digest := sha256.New()
	for _, relative := range paths {
		path := filepath.Join(root, filepath.FromSlash(relative))
		info, err := os.Lstat(path)
		if err != nil {
			return "", err
		}
		_, _ = fmt.Fprintf(digest, "%s\x00%s\x00", relative, info.Mode().String())
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return "", err
			}
			_, _ = io.WriteString(digest, target)
		case info.Mode().IsRegular():
			file, err := os.Open(path)
			if err != nil {
				return "", err
			}
			_, copyErr := io.Copy(digest, file)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				return "", errors.Join(copyErr, closeErr)
			}
		}
		_, _ = digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func modulePath(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open go.mod: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "module" && !strings.ContainsAny(fields[1], "\r\n\x00 ") {
			return fields[1], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read go.mod: %w", err)
	}
	return "", errors.New("go.mod module path is missing")
}

func gitOutput(root string, arguments ...string) (string, error) {
	return commandOutput("", "git", append([]string{"-C", root}, arguments...)...)
}

func commandOutput(directory, name string, arguments ...string) (string, error) {
	command := exec.Command(name, arguments...)
	command.Dir = directory
	command.Env = append(os.Environ(),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_SYSTEM="+os.DevNull,
		"GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_TERMINAL_PROMPT=0",
		"GOPROXY=off",
		"GOSUMDB=off",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %v: %w: %s", name, arguments, err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}
