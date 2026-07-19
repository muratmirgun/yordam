// Package foundationfixture provides byte-exact Foundation acceptance fixtures.
package foundationfixture

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func Names() []string {
	names := make([]string, len(definitions))
	for i, value := range definitions {
		names[i] = value.name
	}
	sort.Strings(names)
	return names
}

func Digest() string {
	values := append([]definition(nil), definitions...)
	sort.Slice(values, func(i, j int) bool { return values[i].name < values[j].name })

	hash := sha256.New()
	for _, value := range values {
		files := append([]fileDefinition(nil), value.files...)
		sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
		for _, file := range files {
			fileHash := sha256.Sum256([]byte(file.data))
			_, _ = hash.Write([]byte(value.name + "/" + file.path))
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write(fileHash[:])
			_, _ = hash.Write([]byte{'\n'})
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func Read(name, relative string) ([]byte, bool) {
	value := definitionByName(name)
	if value.name == "" {
		return nil, false
	}
	for _, file := range value.files {
		if file.path == relative {
			return []byte(file.data), true
		}
	}
	return nil, false
}

func Materialize(root string, names ...string) error {
	if !filepath.IsAbs(root) {
		return fmt.Errorf("foundation fixture root must be absolute: %q", root)
	}
	if err := validateDefinitions(definitions); err != nil {
		return fmt.Errorf("validate foundation fixtures: %w", err)
	}
	if len(names) == 0 {
		names = Names()
	}

	selected := make([]definition, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate foundation fixture name: %q", name)
		}
		seen[name] = struct{}{}
		value := definitionByName(name)
		if value.name == "" {
			return fmt.Errorf("unknown foundation fixture name: %q", name)
		}
		selected = append(selected, value)
	}

	for _, value := range selected {
		for _, file := range value.files {
			path := filepath.Join(root, value.name, file.path)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return fmt.Errorf("create fixture directory %q: %w", filepath.Dir(path), err)
			}
			if err := os.WriteFile(path, []byte(file.data), 0o644); err != nil {
				return fmt.Errorf("write fixture file %q: %w", path, err)
			}
		}
	}
	return nil
}

func definitionByName(name string) definition {
	for _, value := range definitions {
		if value.name == name {
			return value
		}
	}
	return definition{}
}

func validateDefinitions(values []definition) error {
	names := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.name == "" {
			return fmt.Errorf("fixture name is empty")
		}
		if _, duplicate := names[value.name]; duplicate {
			return fmt.Errorf("duplicate fixture name: %q", value.name)
		}
		names[value.name] = struct{}{}

		paths := make(map[string]struct{}, len(value.files))
		for _, file := range value.files {
			if err := validateRelativePath(file.path); err != nil {
				return fmt.Errorf("fixture %q: %w", value.name, err)
			}
			if _, duplicate := paths[file.path]; duplicate {
				return fmt.Errorf("fixture %q: duplicate path %q", value.name, file.path)
			}
			paths[file.path] = struct{}{}
		}
	}
	return nil
}

func validateRelativePath(path string) error {
	if path == "" || filepath.IsAbs(path) {
		return fmt.Errorf("path must be a non-empty relative path: %q", path)
	}
	clean := filepath.Clean(path)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes fixture root: %q", path)
	}
	return nil
}
