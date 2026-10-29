package azauth

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

func TestNewCredential(t *testing.T) {
	const tenant, client = "22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333333"
	cases := []struct {
		name string
		cfg  Config
		want string // credential type, or "error"
	}{
		{"system-assigned managed identity", Config{Method: ManagedIdentity}, "*azidentity.ManagedIdentityCredential"},
		{"user-assigned managed identity", Config{Method: ManagedIdentity, ClientID: client}, "*azidentity.ManagedIdentityCredential"},
		{"service principal", Config{Method: ServicePrincipal, TenantID: tenant, ClientID: client, ClientSecret: "s"}, "*azidentity.ClientSecretCredential"},
		{"service principal without secret", Config{Method: ServicePrincipal, TenantID: tenant, ClientID: client}, "error"},
		{"password", Config{Method: Password, ClientID: client, User: "u@example.com", UserPassword: "p"}, "*azidentity.UsernamePasswordCredential"},
		{"password without client_id", Config{Method: Password, User: "u@example.com", UserPassword: "p"}, "error"},
		{"empty method", Config{}, "error"},
		{"sql is not an Entra method", Config{Method: "sql"}, "error"},
	}
	for _, tc := range cases {
		cred, err := NewCredential(tc.cfg)
		got := "error"
		if err == nil {
			got = fmt.Sprintf("%T", cred)
		}
		if got != tc.want {
			t.Errorf("%s: NewCredential = %s (err %v), want %s", tc.name, got, err, tc.want)
		}
	}
}

type fakeCred struct {
	scopes []string
	err    error
}

func (f *fakeCred) GetToken(_ context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	f.scopes = opts.Scopes
	return azcore.AccessToken{Token: "tok"}, f.err
}

func TestTokenFunc(t *testing.T) {
	cred := &fakeCred{}
	tok, err := TokenFunc(cred, "https://database.windows.net/.default")(context.Background())
	if err != nil || tok != "tok" {
		t.Fatalf("TokenFunc = %q, %v; want tok", tok, err)
	}
	if len(cred.scopes) != 1 || cred.scopes[0] != "https://database.windows.net/.default" {
		t.Errorf("scopes = %v", cred.scopes)
	}
	cred.err = errors.New("denied")
	if _, err := TokenFunc(cred, "s")(context.Background()); err == nil {
		t.Error("TokenFunc: want the credential's error")
	}
}
