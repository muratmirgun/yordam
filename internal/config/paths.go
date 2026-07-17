package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func DefaultConfigPath() (string, error) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return "", fmt.Errorf("unsupported operating system %s", runtime.GOOS)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "yordam", "config.jsonc"), nil
}

func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "yordam", "data"), nil
	case "linux":
		if dir := os.Getenv("XDG_DATA_HOME"); dir != "" {
			return filepath.Join(dir, "yordam"), nil
		}
		return filepath.Join(home, ".local", "share", "yordam"), nil
	default:
		return "", fmt.Errorf("unsupported operating system %s", runtime.GOOS)
	}
}
