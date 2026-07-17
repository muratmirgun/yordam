package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var saveMu sync.Mutex

const abandonedTemporaryAge = 24 * time.Hour

var defaultTemplate = []byte(`{
  "$schema": "` + SchemaURL + `",

  // Format: provider/model
  "model": "openai/your-model-id",

  "provider": {
    "openai": {
      "name": "OpenAI",
      "options": {
        "baseURL": "https://api.openai.com/v1",
        "apiKeyEnv": "OPENAI_API_KEY"
      },
      "models": {
        "your-model-id": {
          "name": "Your model"
        }
      }
    }
  },

  "limits": {
    "maxToolCalls": 32,
    "shellTimeoutSeconds": 120
  }
}
`)

func EnsureGlobal() (path string, created bool, err error) {
	return ensureGlobal(os.Link, os.Remove, syncDirectory)
}

func ensureGlobal(
	linkFile func(string, string) error,
	removeFile func(string) error,
	syncDir func(string) error,
) (path string, created bool, err error) {
	saveMu.Lock()
	defer saveMu.Unlock()
	path, err = DefaultConfigPath()
	if err != nil {
		return "", false, err
	}
	if _, err := os.Lstat(path); err == nil {
		return path, false, nil
	} else if !os.IsNotExist(err) {
		return "", false, err
	}

	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", false, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", false, err
	}
	if err := cleanupConfigTemporaries(directory); err != nil {
		return "", false, err
	}
	temporary, err := os.CreateTemp(directory, ".config-*.tmp")
	if err != nil {
		return "", false, err
	}
	temporaryPath := temporary.Name()
	temporaryClosed := false
	defer func() {
		if !temporaryClosed {
			err = errors.Join(err, temporary.Close())
		}
		err = errors.Join(err, removeConfigTemporary(temporaryPath, directory, removeFile, syncDir))
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return "", false, err
	}
	if _, err := temporary.Write(defaultTemplate); err != nil {
		return "", false, err
	}
	if err := temporary.Sync(); err != nil {
		return "", false, err
	}
	closeErr := temporary.Close()
	temporaryClosed = true
	if closeErr != nil {
		return "", false, closeErr
	}
	if err := linkFile(temporaryPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return path, false, nil
		}
		return "", false, err
	}
	return path, true, nil
}

func SaveGlobal(path string, cfg Config) (err error) {
	saveMu.Lock()
	defer saveMu.Unlock()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("config path is empty")
	}
	raw, err := marshalConfig(cfg)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	if err := cleanupConfigTemporaries(directory); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".config-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	temporaryClosed := false
	temporaryMoved := false
	defer func() {
		if !temporaryClosed {
			err = errors.Join(err, temporary.Close())
		}
		if !temporaryMoved {
			err = errors.Join(err, removeConfigTemporary(temporaryPath, directory, os.Remove, syncDirectory))
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err = temporary.Write(raw); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	closeErr := temporary.Close()
	temporaryClosed = true
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return err
	}
	temporaryMoved = true
	return syncDirectory(directory)
}

func marshalConfig(cfg Config) ([]byte, error) {
	doc := document{
		Schema:   SchemaURL,
		Model:    cfg.ActiveProfile + "/" + cfg.Profiles[cfg.ActiveProfile].DefaultModel,
		Provider: make(map[string]documentProvider, len(cfg.Profiles)),
		Limits: documentLimits{
			MaxToolCalls:        &cfg.MaxToolCalls,
			ShellTimeoutSeconds: &cfg.ShellTimeoutSeconds,
		},
	}
	for name, profile := range cfg.Profiles {
		models := make(map[string]documentModel, len(profile.Models))
		ordered := append([]string(nil), profile.Models...)
		sort.Strings(ordered)
		for _, model := range ordered {
			models[model] = documentModel{}
		}
		doc.Provider[name] = documentProvider{
			Name: profile.Name,
			Options: documentProviderOptions{
				BaseURL:   profile.BaseURL,
				APIKeyEnv: profile.APIKeyEnv,
			},
			Models: models,
		}
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	return append(raw, '\n'), err
}

func cleanupConfigTemporaries(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".config-") || !strings.HasSuffix(name, ".tmp") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || time.Since(info.ModTime()) < abandonedTemporaryAge {
			continue
		}
		if err := os.Remove(filepath.Join(directory, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
		removed = true
	}
	if !removed {
		return nil
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(directoryFile.Sync(), directoryFile.Close())
}

func syncDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(directoryFile.Sync(), directoryFile.Close())
}

func removeConfigTemporary(
	temporaryPath string,
	directory string,
	removeFile func(string) error,
	syncDir func(string) error,
) error {
	removeErr := removeFile(temporaryPath)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(removeErr, syncDir(directory))
}
