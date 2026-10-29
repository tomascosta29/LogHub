// Package azauth builds the Microsoft Entra (Azure AD) credential for a
// plugin's auth method, so the mssql source and the sentinel destination
// accept the same method names and behave the same way.
package azauth

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

// Auth methods.
const (
	ManagedIdentity  = "ad-managed-identity"
	ServicePrincipal = "ad-service-principal"
	Password         = "ad-password"
)

// Config holds the auth properties a plugin collected from its config.
type Config struct {
	Method string
	// TenantID is required for ServicePrincipal. For Password it defaults
	// to "organizations" (any work or school account).
	TenantID string
	// ClientID selects a user-assigned identity for ManagedIdentity, and
	// is the application ID for ServicePrincipal and Password.
	ClientID     string
	ClientSecret string
	User         string
	UserPassword string
}

// NewCredential returns the credential for cfg.Method:
//
//   - ManagedIdentity: system-assigned, or user-assigned when ClientID is
//     set. Only available on an Azure host.
//   - ServicePrincipal: client secret; works anywhere, e.g. for tests
//     from a workstation.
//   - Password: a user's name and password. It cannot satisfy MFA and
//     Microsoft has deprecated it; prefer the other two.
func NewCredential(cfg Config) (azcore.TokenCredential, error) {
	switch cfg.Method {
	case ManagedIdentity:
		var opts *azidentity.ManagedIdentityCredentialOptions
		if cfg.ClientID != "" {
			opts = &azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(cfg.ClientID)}
		}
		cred, err := azidentity.NewManagedIdentityCredential(opts)
		if err != nil {
			return nil, fmt.Errorf("managed identity credential: %w", err)
		}
		return cred, nil
	case ServicePrincipal:
		if cfg.TenantID == "" || cfg.ClientID == "" || cfg.ClientSecret == "" {
			return nil, fmt.Errorf("auth method %q requires tenant_id, client_id and client_secret", cfg.Method)
		}
		cred, err := azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("service principal credential: %w", err)
		}
		return cred, nil
	case Password:
		if cfg.ClientID == "" || cfg.User == "" || cfg.UserPassword == "" {
			return nil, fmt.Errorf("auth method %q requires client_id, user and password", cfg.Method)
		}
		tenant := cfg.TenantID
		if tenant == "" {
			tenant = "organizations"
		}
		cred, err := azidentity.NewUsernamePasswordCredential(tenant, cfg.ClientID, cfg.User, cfg.UserPassword, nil)
		if err != nil {
			return nil, fmt.Errorf("password credential: %w", err)
		}
		return cred, nil
	default:
		return nil, fmt.Errorf("unsupported auth method %q", cfg.Method)
	}
}

// TokenFunc returns a function that fetches an access token for scope
// from cred. azidentity caches and refreshes tokens, so it is cheap to
// call for every connection or request.
func TokenFunc(cred azcore.TokenCredential, scope string) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		tok, err := cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{scope}})
		if err != nil {
			return "", fmt.Errorf("token acquisition failed: %w", err)
		}
		return tok.Token, nil
	}
}
