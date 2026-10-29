package secrets

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"loghub/internal/metrics"
)

func TestEnvProvider(t *testing.T) {
	os.Setenv("TEST_SECRET", "secret-value")
	defer os.Unsetenv("TEST_SECRET")

	provider := &EnvProvider{}

	t.Run("GetSecret", func(t *testing.T) {
		val, err := provider.GetSecret(context.Background(), "TEST_SECRET")
		if err != nil {
			t.Fatalf("GetSecret failed: %v", err)
		}
		if val != "secret-value" {
			t.Errorf("Expected 'secret-value', got '%s'", val)
		}
	})

	t.Run("GetSecretNotFound", func(t *testing.T) {
		_, err := provider.GetSecret(context.Background(), "NONEXISTENT")
		if err == nil {
			t.Error("Expected error for missing secret")
		}
	})
}

func TestParseSecretRef(t *testing.T) {
	tests := []struct {
		input   string
		wantRef SecretRef
		wantOK  bool
	}{
		{"${provider:secret}", SecretRef{Provider: "provider", Name: "secret"}, true},
		{"${keyvault:password}", SecretRef{Provider: "keyvault", Name: "password"}, true},
		{"plaintext", SecretRef{}, false},
		{"${}", SecretRef{}, false},
		{"${provider}", SecretRef{}, false},
		{"${:secret}", SecretRef{Provider: "", Name: "secret"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ref, ok := ParseSecretRef(tt.input)
			if ok != tt.wantOK {
				t.Errorf("ParseSecretRef(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if ok && (ref.Provider != tt.wantRef.Provider || ref.Name != tt.wantRef.Name) {
				t.Errorf("ParseSecretRef(%q) = %+v, want %+v", tt.input, ref, tt.wantRef)
			}
		})
	}
}

func TestSecretRefString(t *testing.T) {
	ref := SecretRef{Provider: "kv", Name: "password"}
	if ref.String() != "${kv:password}" {
		t.Errorf("Unexpected string: %s", ref.String())
	}
}

func TestResolveSecretNotRef(t *testing.T) {
	providers := map[string]Provider{}

	val, err := ResolveSecret(context.Background(), "plaintext", providers)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if val != "plaintext" {
		t.Errorf("Expected plaintext, got %s", val)
	}
}

func TestResolveSecretProviderNotFound(t *testing.T) {
	providers := map[string]Provider{}

	_, err := ResolveSecret(context.Background(), "${nonexistent:secret}", providers)
	if err == nil {
		t.Error("Expected error for missing provider")
	}
}

func TestNewSecretHolder(t *testing.T) {
	envProvider := &EnvProvider{}

	holder, err := NewSecretHolder(map[string]ProviderConfig{
		"env": {Type: "env"},
	})
	if err != nil {
		t.Fatalf("NewSecretHolder failed: %v", err)
	}
	holder.providers["env"] = envProvider

	os.Setenv("TEST_SECRET", "test-value")
	defer os.Unsetenv("TEST_SECRET")

	val, err := holder.Resolve(context.Background(), "${env:TEST_SECRET}")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if val != "test-value" {
		t.Errorf("Expected 'test-value', got '%s'", val)
	}
}

func TestNewSecretHolderUnknownType(t *testing.T) {
	_, err := NewSecretHolder(map[string]ProviderConfig{
		"unknown": {Type: "unknown"},
	})
	if err == nil {
		t.Error("Expected error for unknown provider type")
	}
}

func TestResolveIteratesMap(t *testing.T) {
	// The production caller iterates over the properties and calls
	// Resolve per value. This test exercises the same pattern
	// directly.
	holder := &SecretHolder{
		providers: map[string]Provider{
			"env": &EnvProvider{},
		},
	}

	os.Setenv("KEY1", "value1")
	defer os.Unsetenv("KEY1")

	ctx := context.Background()
	inputs := map[string]string{
		"key1": "plaintext",
		"key2": "${env:KEY1}",
	}
	for k, v := range inputs {
		got, err := holder.Resolve(ctx, v)
		if err != nil {
			t.Fatalf("Resolve %s: %v", k, err)
		}
		switch k {
		case "key1":
			if got != "plaintext" {
				t.Errorf("%s = %q, want plaintext", k, got)
			}
		case "key2":
			if got != "value1" {
				t.Errorf("%s = %q, want value1", k, got)
			}
		}
	}
}

func TestSecretHolderStrictModeUnresolvedSecret(t *testing.T) {
	holder := &SecretHolder{
		providers: map[string]Provider{
			"env": &EnvProvider{},
		},
		Strict: true,
	}

	os.Setenv("TEST_SECRET", "test-value")
	defer os.Unsetenv("TEST_SECRET")

	ctx := context.Background()

	t.Run("PlaintextReturnsAsIs", func(t *testing.T) {
		val, err := holder.Resolve(ctx, "plaintext")
		if err != nil {
			t.Fatalf("Unexpected error for plaintext: %v", err)
		}
		if val != "plaintext" {
			t.Errorf("Expected plaintext, got %s", val)
		}
	})

	t.Run("ResolvedSecretSucceeds", func(t *testing.T) {
		val, err := holder.Resolve(ctx, "${env:TEST_SECRET}")
		if err != nil {
			t.Fatalf("Unexpected error for resolved secret: %v", err)
		}
		if val != "test-value" {
			t.Errorf("Expected test-value, got %s", val)
		}
	})

	t.Run("UnresolvedSecretReturnsError", func(t *testing.T) {
		_, err := holder.Resolve(ctx, "${nonexistent:secret}")
		if err == nil {
			t.Error("Expected error for unresolved secret in strict mode")
		}
	})

	t.Run("NonExistentProviderReturnsError", func(t *testing.T) {
		_, err := holder.Resolve(ctx, "${unknown:secret}")
		if err == nil {
			t.Error("Expected error for non-existent provider in strict mode")
		}
	})
}

func TestSecretHolderNonStrictMode(t *testing.T) {
	holder := &SecretHolder{
		providers: map[string]Provider{
			"env": &EnvProvider{},
		},
		Strict: false,
	}

	ctx := context.Background()

	t.Run("UnresolvedSecretReturnsOriginal", func(t *testing.T) {
		val, err := holder.Resolve(ctx, "${nonexistent:secret}")
		if err != nil {
			t.Fatalf("Unexpected error in non-strict mode: %v", err)
		}
		if val != "${nonexistent:secret}" {
			t.Errorf("Expected original value, got %s", val)
		}
	})

	t.Run("NonExistentProviderReturnsOriginal", func(t *testing.T) {
		val, err := holder.Resolve(ctx, "${unknown:secret}")
		if err != nil {
			t.Fatalf("Unexpected error in non-strict mode: %v", err)
		}
		if val != "${unknown:secret}" {
			t.Errorf("Expected original value, got %s", val)
		}
	})
}

// fakeProvider returns value, or err when set. Safe for concurrent use.
type fakeProvider struct {
	mu    sync.Mutex
	value string
	err   error
	calls int
}

func (f *fakeProvider) GetSecret(ctx context.Context, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	return f.value, nil
}

func (f *fakeProvider) set(value string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.value, f.err = value, err
}

func newFakeCache(strict bool) (*CachedSecretHolder, *fakeProvider) {
	fp := &fakeProvider{value: "v1"}
	inner := &SecretHolder{providers: map[string]Provider{"fake": fp}, Strict: strict}
	return NewCachedSecretHolder(inner, time.Minute), fp
}

const fakeRef = "${fake:db-password}"

// Once a secret has resolved, a failed lookup keeps serving that
// value instead of failing (strict) or returning the raw reference.
func TestCachedSecretHolderFallsBackToLastValue(t *testing.T) {
	for _, strict := range []bool{true, false} {
		c, fp := newFakeCache(strict)
		if v, err := c.Resolve(context.Background(), fakeRef); err != nil || v != "v1" {
			t.Fatalf("strict=%v: first Resolve = %q, %v; want v1", strict, v, err)
		}
		fp.set("", errors.New("vault unreachable"))
		c.now = func() time.Time { return time.Now().Add(time.Hour) } // expired
		errsBefore := testutil.ToFloat64(metrics.SecretRefreshErrorsTotal.WithLabelValues("fake"))

		v, err := c.Resolve(context.Background(), fakeRef)
		if err != nil || v != "v1" {
			t.Errorf("strict=%v: Resolve during outage = %q, %v; want the last value v1", strict, v, err)
		}
		if got := testutil.ToFloat64(metrics.SecretRefreshErrorsTotal.WithLabelValues("fake")) - errsBefore; got != 1 {
			t.Errorf("strict=%v: secret_refresh_errors_total rose by %v, want 1", strict, got)
		}
	}
}

// A failure was cached for the whole TTL in non-strict mode, so the raw
// reference kept being used after the vault recovered.
func TestCachedSecretHolderDoesNotCacheFailures(t *testing.T) {
	c, fp := newFakeCache(false)
	fp.set("", errors.New("vault unreachable"))
	if v, err := c.Resolve(context.Background(), fakeRef); err != nil || v != fakeRef {
		t.Fatalf("Resolve during outage = %q, %v; want the raw reference (non-strict)", v, err)
	}
	fp.set("v1", nil)
	if v, err := c.Resolve(context.Background(), fakeRef); err != nil || v != "v1" {
		t.Errorf("Resolve after recovery = %q, %v; want v1", v, err)
	}
}

func TestCachedSecretHolderStrictFailsWithoutPreviousValue(t *testing.T) {
	c, fp := newFakeCache(true)
	fp.set("", errors.New("vault unreachable"))
	if _, err := c.Resolve(context.Background(), fakeRef); err == nil {
		t.Error("Resolve = nil error, want an error in strict mode with no previous value")
	}
}

// Refresh looks secrets up again even when the cache is fresh, reports
// which references changed (not their values), and keeps the previous
// value of a secret that fails.
func TestCachedSecretHolderRefresh(t *testing.T) {
	c, fp := newFakeCache(true)
	if _, err := c.Resolve(context.Background(), fakeRef); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	fp.set("v2", nil)
	changed, err := c.Refresh(context.Background())
	if err != nil || len(changed) != 1 || changed[0] != fakeRef {
		t.Fatalf("Refresh = %v, %v; want [%s], nil", changed, err, fakeRef)
	}
	if v, _ := c.Resolve(context.Background(), fakeRef); v != "v2" {
		t.Errorf("Resolve after Refresh = %q, want v2 (without waiting for the TTL)", v)
	}

	if changed, err := c.Refresh(context.Background()); err != nil || len(changed) != 0 {
		t.Errorf("Refresh with no change = %v, %v; want none", changed, err)
	}

	fp.set("", errors.New("vault unreachable"))
	if _, err := c.Refresh(context.Background()); err == nil {
		t.Error("Refresh during outage returned nil error")
	}
	if v, _ := c.Resolve(context.Background(), fakeRef); v != "v2" {
		t.Errorf("Resolve after a failed Refresh = %q, want v2 kept", v)
	}
}

func TestCachedSecretHolderBackgroundRefreshReportsChange(t *testing.T) {
	c, fp := newFakeCache(true)
	if _, err := c.Resolve(context.Background(), fakeRef); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	notified := make(chan []string, 1)
	c.StartRefresh(10*time.Millisecond, func(changed []string) {
		select {
		case notified <- changed:
		default:
		}
	})
	defer c.Close()

	fp.set("v2", nil)
	select {
	case changed := <-notified:
		if len(changed) != 1 || changed[0] != fakeRef {
			t.Errorf("onChange got %v, want [%s]", changed, fakeRef)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not report the changed secret")
	}
}

// Close stops the background refresh: no lookups after it returns.
func TestCachedSecretHolderCloseStopsRefresh(t *testing.T) {
	c, fp := newFakeCache(true)
	if _, err := c.Resolve(context.Background(), fakeRef); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	c.StartRefresh(5*time.Millisecond, nil)
	time.Sleep(30 * time.Millisecond)
	c.Close()
	fp.mu.Lock()
	calls := fp.calls
	fp.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if fp.calls != calls {
		t.Errorf("provider called %d more times after Close", fp.calls-calls)
	}
}

func TestDefaultCredentialOptionsUsesTenantID(t *testing.T) {
	if opts := defaultCredentialOptions(ProviderConfig{}); opts != nil {
		t.Errorf("options without tenant_id = %+v, want nil", opts)
	}
	opts := defaultCredentialOptions(ProviderConfig{TenantID: "tenant-a"})
	if opts == nil || opts.TenantID != "tenant-a" {
		t.Errorf("options with tenant_id = %+v, want TenantID tenant-a", opts)
	}
}
