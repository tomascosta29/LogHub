package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"
	"gopkg.in/yaml.v3"

	"loghub/internal/metrics"
)

type Provider interface {
	GetSecret(ctx context.Context, name string) (string, error)
}

// Resolver is the contract that callers (sources, destinations,
// processors, the resolveProperties helper) depend on. Both
// *SecretHolder and *CachedSecretHolder satisfy it. The
// cmd/loghub/main.go orchestrator wraps the live *SecretHolder in a
// *CachedSecretHolder so secret resolution is cached per
// (secret_ref, TTL).
type Resolver interface {
	Resolve(ctx context.Context, value string) (string, error)
}

type Config map[string]ProviderConfig

type ProviderConfig struct {
	Type     string `yaml:"type"`
	VaultURL string `yaml:"vault_url"`
	TenantID string `yaml:"tenant_id"`
	ClientID string `yaml:"client_id"`
}

func NewProvider(ctx context.Context, cfg ProviderConfig) (Provider, error) {
	switch cfg.Type {
	case "keyvault":
		return NewKeyVaultProvider(ctx, cfg)
	case "env":
		return &EnvProvider{}, nil
	default:
		return nil, fmt.Errorf("unknown secrets provider type: %s", cfg.Type)
	}
}

type KeyVaultProvider struct {
	client *azsecrets.Client
}

func NewKeyVaultProvider(ctx context.Context, cfg ProviderConfig) (*KeyVaultProvider, error) {
	var cred azcore.TokenCredential
	var err error

	if cfg.ClientID != "" {
		cred, err = azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
			ID: azidentity.ClientID(cfg.ClientID),
		})
	} else {
		cred, err = azidentity.NewDefaultAzureCredential(nil)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create credential: %w", err)
	}

	client, err := azsecrets.NewClient(cfg.VaultURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create keyvault client: %w", err)
	}

	return &KeyVaultProvider{client: client}, nil
}

func (p *KeyVaultProvider) GetSecret(ctx context.Context, name string) (string, error) {
	resp, err := p.client.GetSecret(ctx, name, "", nil)
	if err != nil {
		return "", fmt.Errorf("failed to get secret %s: %w", name, err)
	}
	if resp.Value == nil {
		return "", fmt.Errorf("secret %s has nil value", name)
	}
	return *resp.Value, nil
}

type EnvProvider struct{}

func (p *EnvProvider) GetSecret(ctx context.Context, name string) (string, error) {
	val := os.Getenv(name)
	if val == "" {
		return "", fmt.Errorf("environment variable %s not found", name)
	}
	return val, nil
}

type SecretRef struct {
	Provider string `yaml:"provider"`
	Name     string `yaml:"name"`
}

func (s SecretRef) String() string {
	return fmt.Sprintf("${%s:%s}", s.Provider, s.Name)
}

func ParseSecretRef(value string) (SecretRef, bool) {
	if !strings.HasPrefix(value, "${") || !strings.HasSuffix(value, "}") {
		return SecretRef{}, false
	}
	inner := value[2 : len(value)-1]
	parts := strings.SplitN(inner, ":", 2)
	if len(parts) != 2 {
		return SecretRef{}, false
	}
	return SecretRef{Provider: parts[0], Name: parts[1]}, true
}

func ResolveSecret(ctx context.Context, value string, providers map[string]Provider) (string, error) {
	ref, ok := ParseSecretRef(value)
	if !ok {
		return value, nil
	}

	provider, exists := providers[ref.Provider]
	if !exists {
		return "", fmt.Errorf("secrets provider %s not found", ref.Provider)
	}

	return provider.GetSecret(ctx, ref.Name)
}

type SecretHolder struct {
	providers map[string]Provider
	Strict    bool
}

func NewSecretHolder(configs map[string]ProviderConfig) (*SecretHolder, error) {
	ctx := context.Background()
	providers := make(map[string]Provider)

	for name, cfg := range configs {
		provider, err := NewProvider(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create provider %s: %w", name, err)
		}
		providers[name] = provider
	}

	return &SecretHolder{providers: providers}, nil
}

func (s *SecretHolder) Resolve(ctx context.Context, value string) (string, error) {
	ref, ok := ParseSecretRef(value)
	if !ok {
		return value, nil
	}
	resolved, err := s.fetch(ctx, ref)
	if err != nil {
		if s.Strict {
			return "", err
		}
		return value, nil
	}
	return resolved, nil
}

// fetch looks up ref in its provider, failing whatever the Strict setting.
func (s *SecretHolder) fetch(ctx context.Context, ref SecretRef) (string, error) {
	provider, exists := s.providers[ref.Provider]
	if !exists {
		return "", fmt.Errorf("secrets provider %s not found", ref.Provider)
	}
	resolved, err := provider.GetSecret(ctx, ref.Name)
	if err != nil {
		return "", fmt.Errorf("failed to resolve secret %s: %w", ref.String(), err)
	}
	return resolved, nil
}

// CachedSecretHolder caches resolved secrets for ttl. Secrets are read
// when pipelines are built (at startup and on reload), so a rotated secret
// takes effect on the next reload; Refresh, run by reloads and by the
// background refresh, keeps the cache current.
//
// A failed lookup never replaces a value: the last resolved value is served
// instead (logged, and counted in loghub_secret_refresh_errors_total). Only
// a reference that never resolved follows the inner holder's Strict policy.
type CachedSecretHolder struct {
	inner *SecretHolder
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]cachedEntry
	now   func() time.Time

	stopRefresh context.CancelFunc
	refreshDone chan struct{}
}

type cachedEntry struct {
	value     string
	expiresAt time.Time
}

func NewCachedSecretHolder(inner *SecretHolder, ttl time.Duration) *CachedSecretHolder {
	return &CachedSecretHolder{
		inner: inner,
		ttl:   ttl,
		cache: make(map[string]cachedEntry),
		now:   time.Now,
	}
}

func (c *CachedSecretHolder) Resolve(ctx context.Context, value string) (string, error) {
	ref, ok := ParseSecretRef(value)
	if !ok {
		return value, nil
	}
	c.mu.Lock()
	e, cached := c.cache[value]
	c.mu.Unlock()
	if cached && c.now().Before(e.expiresAt) {
		return e.value, nil
	}

	resolved, err := c.lookup(ctx, value, ref)
	if err == nil {
		return resolved, nil
	}
	if cached {
		slog.Warn("Secret lookup failed; using the last resolved value", "secret", value, "error", err)
		return e.value, nil
	}
	if c.inner.Strict {
		return "", err
	}
	return value, nil
}

// lookup fetches ref, counts it, and caches the value on success.
func (c *CachedSecretHolder) lookup(ctx context.Context, value string, ref SecretRef) (string, error) {
	metrics.SecretRefreshTotal.WithLabelValues(ref.Provider).Inc()
	resolved, err := c.inner.fetch(ctx, ref)
	if err != nil {
		metrics.SecretRefreshErrorsTotal.WithLabelValues(ref.Provider).Inc()
		return "", err
	}
	c.mu.Lock()
	c.cache[value] = cachedEntry{value: resolved, expiresAt: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return resolved, nil
}

// Refresh looks up every cached secret again, whether or not it expired.
// It returns the references whose value changed, and the lookup errors
// joined; a secret that failed keeps its previous value.
func (c *CachedSecretHolder) Refresh(ctx context.Context) (changed []string, err error) {
	c.mu.Lock()
	previous := make(map[string]string, len(c.cache))
	for value, e := range c.cache {
		previous[value] = e.value
	}
	c.mu.Unlock()

	refs := make([]string, 0, len(previous))
	for value := range previous {
		refs = append(refs, value)
	}
	sort.Strings(refs)

	var errs []error
	for _, value := range refs {
		ref, _ := ParseSecretRef(value)
		resolved, lookupErr := c.lookup(ctx, value, ref)
		if lookupErr != nil {
			errs = append(errs, lookupErr)
			continue
		}
		if resolved != previous[value] {
			changed = append(changed, value)
		}
	}
	return changed, errors.Join(errs...)
}

// StartRefresh runs Refresh every interval until Close. It calls onChange
// with the changed references (never their values) when a value changes.
// Lookup errors are logged; their previous values stay in use.
func (c *CachedSecretHolder) StartRefresh(interval time.Duration, onChange func(changed []string)) {
	ctx, cancel := context.WithCancel(context.Background())
	c.stopRefresh = cancel
	c.refreshDone = make(chan struct{})
	go func() {
		defer close(c.refreshDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			changed, err := c.Refresh(ctx)
			if err != nil && ctx.Err() == nil {
				slog.Warn("Background secret refresh failed; keeping the last resolved values", "error", err)
			}
			if len(changed) > 0 && onChange != nil {
				onChange(changed)
			}
		}
	}()
}

// Close stops the background refresh, if started, and waits for it.
func (c *CachedSecretHolder) Close() {
	if c.stopRefresh != nil {
		c.stopRefresh()
		<-c.refreshDone
	}
	c.inner.Close()
}

func (s *SecretHolder) Close() {}

type SecretValue struct {
	Value    string
	Resolved bool
}

func ParseConfigSecrets(data []byte) (map[string]ProviderConfig, error) {
	var cfg struct {
		Secrets map[string]ProviderConfig `yaml:"secrets"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return cfg.Secrets, nil
}

func MarshalJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
