package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/tailscale/hujson"
)

const SchemaURL = "https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json"

const (
	defaultMaxToolCalls        = 32
	defaultShellTimeoutSeconds = 120
	reservedModelID            = "your-model-id"
)

type Profile struct {
	Name         string
	BaseURL      string
	APIKeyEnv    string
	Models       []string
	DefaultModel string
}

type Config struct {
	ActiveProfile       string
	Profiles            map[string]Profile
	MaxToolCalls        int
	ShellTimeoutSeconds int
	raw                 []byte
	lookupEnv           func(string) (string, bool)
}

type LoadOptions struct {
	ConfigPath string
	LookupEnv  func(string) (string, bool)
}

type ResolveOptions struct {
	Profile        string
	Model          string
	BaseURL        string
	ProcessAPIKey  string // removed with the setup stage in Task 5
	DefaultProfile string
	DefaultModel   string
}

type ResolvedProfile struct {
	Name      string
	Label     string
	BaseURL   string
	APIKey    string
	APIKeyEnv string
	Model     string
}

type document struct {
	Schema   string                      `json:"$schema,omitempty"`
	Model    string                      `json:"model"`
	Provider map[string]documentProvider `json:"provider"`
	Limits   documentLimits              `json:"limits,omitempty"`
}

type documentProvider struct {
	Name    string                   `json:"name,omitempty"`
	Options documentProviderOptions  `json:"options"`
	Models  map[string]documentModel `json:"models"`
}

type documentProviderOptions struct {
	BaseURL   string `json:"baseURL"`
	APIKeyEnv string `json:"apiKeyEnv"`
}

type documentModel struct {
	Name string `json:"name,omitempty"`
}

type documentLimits struct {
	MaxToolCalls        *int `json:"maxToolCalls,omitempty"`
	ShellTimeoutSeconds *int `json:"shellTimeoutSeconds,omitempty"`
}

var envName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
var hujsonErrorLocation = regexp.MustCompile(`^hujson: line ([0-9]+), column ([0-9]+):`)

func Load(opts LoadOptions) (Config, error) {
	if opts.LookupEnv == nil {
		opts.LookupEnv = os.LookupEnv
	}
	raw, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		return Config{}, err
	}
	value, err := hujson.Parse(raw)
	if err != nil {
		if location := hujsonErrorLocation.FindStringSubmatch(err.Error()); len(location) == 3 {
			return Config{}, fmt.Errorf("invalid JSONC at line %s, column %s", location[1], location[2])
		}
		return Config{}, fmt.Errorf("invalid JSONC")
	}
	value.Standardize()
	standardized := value.Pack()
	var decoded document
	decoder := json.NewDecoder(bytes.NewReader(standardized))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return Config{}, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Config{}, err
	}
	cfg, err := normalizeDocument(decoded)
	if err != nil {
		return Config{}, err
	}
	cfg.raw = append([]byte(nil), raw...)
	cfg.lookupEnv = opts.LookupEnv
	return cfg, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("config contains multiple JSON values")
		}
		return err
	}
	return nil
}

func normalizeDocument(decoded document) (Config, error) {
	providerID, modelID, ok := splitSelection(decoded.Model)
	if !ok {
		return Config{}, fmt.Errorf("model must use provider/model format")
	}
	if len(decoded.Provider) == 0 {
		return Config{}, fmt.Errorf("provider must contain at least one entry")
	}
	cfg := Config{
		ActiveProfile:       providerID,
		Profiles:            make(map[string]Profile, len(decoded.Provider)),
		MaxToolCalls:        defaultMaxToolCalls,
		ShellTimeoutSeconds: defaultShellTimeoutSeconds,
	}
	if decoded.Limits.MaxToolCalls != nil {
		cfg.MaxToolCalls = *decoded.Limits.MaxToolCalls
	}
	if decoded.Limits.ShellTimeoutSeconds != nil {
		cfg.ShellTimeoutSeconds = *decoded.Limits.ShellTimeoutSeconds
	}
	for id, provider := range decoded.Provider {
		if id == "" {
			return Config{}, fmt.Errorf("provider ID is empty")
		}
		models := make([]string, 0, len(provider.Models))
		for model := range provider.Models {
			if model == "" {
				return Config{}, fmt.Errorf("provider %q model ID is empty", id)
			}
			if model == reservedModelID {
				return Config{}, fmt.Errorf("model ID %q is reserved", model)
			}
			models = append(models, model)
		}
		sort.Strings(models)
		defaultModel := ""
		if len(models) > 0 {
			defaultModel = models[0]
		}
		if id == providerID {
			defaultModel = modelID
		}
		cfg.Profiles[id] = Profile{
			Name:         provider.Name,
			BaseURL:      provider.Options.BaseURL,
			APIKeyEnv:    provider.Options.APIKeyEnv,
			Models:       models,
			DefaultModel: defaultModel,
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.MaxToolCalls < 1 || c.MaxToolCalls > 128 {
		return fmt.Errorf("maxToolCalls must be 1..128")
	}
	if c.ShellTimeoutSeconds < 1 || c.ShellTimeoutSeconds > 1800 {
		return fmt.Errorf("shellTimeoutSeconds must be 1..1800")
	}
	profile, ok := c.Profiles[c.ActiveProfile]
	if !ok {
		return fmt.Errorf("provider %q not found", c.ActiveProfile)
	}
	if !slices.Contains(profile.Models, profile.DefaultModel) {
		return fmt.Errorf("model %q not configured for %q", profile.DefaultModel, c.ActiveProfile)
	}
	for name, configured := range c.Profiles {
		if name == "" {
			return fmt.Errorf("provider ID is empty")
		}
		if !validBaseURL(configured.BaseURL) {
			return fmt.Errorf("provider %q has invalid baseURL", name)
		}
		if !envName.MatchString(configured.APIKeyEnv) {
			return fmt.Errorf("provider %q has invalid apiKeyEnv", name)
		}
		if len(configured.Models) == 0 {
			return fmt.Errorf("provider %q has invalid models", name)
		}
		for _, model := range configured.Models {
			if model == "" {
				return fmt.Errorf("provider %q model ID is empty", name)
			}
			if model == reservedModelID {
				return fmt.Errorf("model ID %q is reserved", model)
			}
		}
	}
	return nil
}

func (c Config) Resolve(opts ResolveOptions) (ResolvedProfile, error) {
	lookup := c.lookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	profileName := opts.Profile
	if profileName == "" {
		profileName, _ = lookup("YORDAM_PROFILE")
	}
	if profileName == "" {
		profileName = opts.DefaultProfile
	}
	if profileName == "" {
		profileName = c.ActiveProfile
	}
	profile, ok := c.Profiles[profileName]
	if !ok {
		return ResolvedProfile{}, fmt.Errorf("provider %q not found", profileName)
	}
	model := opts.Model
	if model == "" {
		model, _ = lookup("YORDAM_MODEL")
	}
	if model == "" && opts.DefaultProfile == profileName {
		model = opts.DefaultModel
	}
	if model == "" {
		model = profile.DefaultModel
	}
	if !slices.Contains(profile.Models, model) {
		return ResolvedProfile{}, fmt.Errorf("model %q not configured for %q", model, profileName)
	}
	key := opts.ProcessAPIKey
	if key == "" {
		key, _ = lookup("YORDAM_API_KEY")
	}
	if key == "" {
		key, _ = lookup(profile.APIKeyEnv)
	}
	baseURL := opts.BaseURL
	if baseURL == "" {
		baseURL, _ = lookup("YORDAM_BASE_URL")
	}
	if baseURL == "" {
		baseURL = profile.BaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")
	if !validBaseURL(baseURL) {
		return ResolvedProfile{}, fmt.Errorf("resolved baseURL is invalid")
	}
	return ResolvedProfile{
		Name:      profileName,
		Label:     profile.Name,
		BaseURL:   baseURL,
		APIKey:    key,
		APIKeyEnv: profile.APIKeyEnv,
		Model:     model,
	}, nil
}

func (c Config) DefaultSelection() domain.ModelSelection {
	profile := c.Profiles[c.ActiveProfile]
	return domain.ModelSelection{Profile: c.ActiveProfile, Model: profile.DefaultModel}
}

func (c Config) Models() []domain.ModelSelection {
	profiles := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		profiles = append(profiles, name)
	}
	sort.Strings(profiles)
	var models []domain.ModelSelection
	for _, profile := range profiles {
		configured := append([]string(nil), c.Profiles[profile].Models...)
		sort.Strings(configured)
		for _, model := range configured {
			models = append(models, domain.ModelSelection{Profile: profile, Model: model})
		}
	}
	return models
}

func (c Config) ProviderKeyEnvironmentNames() []string {
	providers := make([]string, 0, len(c.Profiles))
	for provider := range c.Profiles {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	names := make([]string, 0, len(providers))
	for _, provider := range providers {
		names = append(names, c.Profiles[provider].APIKeyEnv)
	}
	return names
}

func (c Config) APIKeys() map[string]string {
	lookup := c.lookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	keys := make(map[string]string, len(c.Profiles))
	for name, profile := range c.Profiles {
		keys[name], _ = lookup(profile.APIKeyEnv)
	}
	return keys
}

func (c Config) RawForTest() []byte { return append([]byte(nil), c.raw...) }

func splitSelection(value string) (string, string, bool) {
	if strings.Count(value, "/") != 1 {
		return "", "", false
	}
	provider, model, _ := strings.Cut(value, "/")
	return provider, model, provider != "" && model != ""
}

func validBaseURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" && parsed.User == nil
}
