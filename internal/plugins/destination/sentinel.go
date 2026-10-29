package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"loghub/internal/azauth"
	"loghub/internal/core"
	"loghub/internal/util"
)

const azureMonitorScope = "https://monitor.azure.com/.default"

type sentinelConfig struct {
	DCE     string `mapstructure:"dce"`
	DCR     string `mapstructure:"dcr"`
	Stream  string `mapstructure:"stream"`
	Timeout string `mapstructure:"timeout"`
	// ClientID selects a user-assigned managed identity when Auth is
	// not set.
	ClientID string          `mapstructure:"client_id"`
	Auth     azureAuthConfig `mapstructure:"auth"`
}

// azureAuthConfig selects how the destination authenticates to Azure
// Monitor. Method is "ad-managed-identity" (the default) or
// "ad-service-principal", matching the mssql source's auth methods.
type azureAuthConfig struct {
	Method       string `mapstructure:"method"`
	TenantID     string `mapstructure:"tenant_id"`
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`
}

func init() {
	factory := func(props map[string]interface{}) (core.Destination, error) {
		var cfg sentinelConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		cred, err := newAzureCredential(cfg)
		if err != nil {
			return nil, err
		}
		return NewSentinelWithTokenProvider(cfg.DCE, cfg.DCR, cfg.Stream, cfg.Timeout, &tokenProvider{cred: cred})
	}
	Register("sentinel", factory)
	Register("azure_monitor", factory)
}

type TokenProvider interface {
	GetToken(ctx context.Context) (string, error)
}

type tokenProvider struct {
	cred azcore.TokenCredential
}

// newAzureCredential builds the credential cfg asks for. Managed
// identity is the default: system-assigned, or user-assigned when a
// client_id is given (top level or inside auth).
func newAzureCredential(cfg sentinelConfig) (azcore.TokenCredential, error) {
	auth := azauth.Config{
		Method:       cfg.Auth.Method,
		TenantID:     cfg.Auth.TenantID,
		ClientID:     cfg.Auth.ClientID,
		ClientSecret: cfg.Auth.ClientSecret,
	}
	if auth.Method == "" {
		auth.Method = azauth.ManagedIdentity
	}
	if auth.Method != azauth.ManagedIdentity && auth.Method != azauth.ServicePrincipal {
		return nil, fmt.Errorf("sentinel destination: unsupported auth method %q", auth.Method)
	}
	if auth.ClientID == "" {
		auth.ClientID = cfg.ClientID
	}
	cred, err := azauth.NewCredential(auth)
	if err != nil {
		return nil, fmt.Errorf("sentinel destination: %w", err)
	}
	return cred, nil
}

func (tp *tokenProvider) GetToken(ctx context.Context) (string, error) {
	azToken, err := tp.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{azureMonitorScope},
	})
	if err != nil {
		return "", fmt.Errorf("token acquisition failed: %w", err)
	}
	return azToken.Token, nil
}

type sentinelDestination struct {
	httpDest      *HTTP
	dce           string
	dcr           string
	stream        string
	tokenProvider TokenProvider
}

func NewSentinelWithTokenProvider(dce, dcr, stream, timeout string, tp TokenProvider) (*sentinelDestination, error) {
	if tp == nil {
		return nil, fmt.Errorf("sentinel destination: token provider is required")
	}
	if dce == "" {
		return nil, fmt.Errorf("sentinel destination: dce is required")
	}
	if dcr == "" {
		return nil, fmt.Errorf("sentinel destination: dcr is required")
	}
	if stream == "" {
		return nil, fmt.Errorf("sentinel destination: stream is required")
	}

	dce = strings.TrimSuffix(dce, "/")

	url := fmt.Sprintf("%s/dataCollectionRules/%s/streams/%s?api-version=2023-01-01",
		dce, dcr, stream)

	to := 30 * time.Second
	if timeout != "" {
		d, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("sentinel destination: invalid timeout: %w", err)
		}
		to = d
	}

	httpDest, err := NewHTTP(url, to)
	if err != nil {
		return nil, err
	}

	return &sentinelDestination{
		httpDest:      httpDest,
		dce:           dce,
		dcr:           dcr,
		stream:        stream,
		tokenProvider: tp,
	}, nil
}

func (s *sentinelDestination) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	if len(logs) == 0 {
		return nil, nil
	}

	token, err := s.tokenProvider.GetToken(ctx)
	if err != nil {
		slog.Warn("Sentinel token acquisition failed", "error", err)
		return nil, fmt.Errorf("sentinel token acquisition failed: %w", err)
	}

	// Pre-validate each entry for a usable timestamp. Entries without one
	// are rejected individually with a poison-pill error so the pusher can
	// DLQ them instead of silently dropping them.
	validEntries := make([]*core.LogEntry, 0, len(logs))
	statuses := make([]core.EntryStatus, len(logs))
	for i, entry := range logs {
		if entry == nil {
			statuses[i] = core.EntryStatus{Index: i, Error: errors.Join(fmt.Errorf("sentinel payload: nil entry"), core.ErrPoisonPill)}
			continue
		}
		if _, err := sentinelTimeGenerated(entry); err != nil {
			statuses[i] = core.EntryStatus{Index: i, Error: errors.Join(err, core.ErrPoisonPill)}
			continue
		}
		validEntries = append(validEntries, entry)
	}

	if len(validEntries) == 0 {
		// All entries rejected; nothing to send.
		return statuses, nil
	}

	data, err := MarshalSentinelPayload(validEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal logs: %w", err)
	}

	rawStatuses, err := s.httpDest.SendRaw(ctx, data, len(validEntries), map[string]string{
		"Authorization": "Bearer " + token,
	})
	if err != nil {
		return nil, err
	}

	// Map the per-entry statuses from the HTTP call back to the original indices.
	validIdx := 0
	for i := range logs {
		if statuses[i].Error == nil {
			// This was a valid entry; copy its status from the HTTP response.
			statuses[i] = rawStatuses[validIdx]
			validIdx++
		}
	}

	return statuses, nil
}

func (s *sentinelDestination) Close() error {
	return s.httpDest.Close()
}

func (s *sentinelDestination) Type() string {
	return "sentinel"
}

func (s *sentinelDestination) SetSourceName(name string) {
	s.httpDest.sourceName = name
}

func BuildSentinelRecords(logs []*core.LogEntry) ([]map[string]string, error) {
	records := make([]map[string]string, 0, len(logs))
	for _, entry := range logs {
		if entry == nil {
			continue
		}
		record := make(map[string]string, len(entry.Fields)+1)
		for k, v := range entry.Fields {
			record[k] = v
		}
		timeGenerated, err := sentinelTimeGenerated(entry)
		if err != nil {
			// Skip entries with no usable timestamp; they are the
			// caller's responsibility to route to the DLQ. We do not
			// fail the whole payload because of one bad entry. The
			// mapping to per-entry rejection is done by the caller in
			// MarshalSentinelPayload.
			continue
		}
		record["TimeGenerated"] = timeGenerated
		records = append(records, record)
	}
	return records, nil
}

// MarshalSentinelPayload is like BuildSentinelRecords but returns the
// JSON bytes for the request body. Entries with missing timestamps are
// left out of the payload; the caller is expected to have
// filtered them out via per-entry pre-checks or to DLQ them after
// observing the mismatch in record count vs input entry count.
//
// This is a backward-compatible signature; the count difference
// between the input slice and the returned payload is the count of
// dropped entries.
func MarshalSentinelPayload(logs []*core.LogEntry) ([]byte, error) {
	records, err := BuildSentinelRecords(logs)
	if err != nil {
		return nil, err
	}
	return json.Marshal(records)
}

// timeGeneratedLayout keeps up to 7 fractional digits, the precision of
// a Log Analytics datetime, so TimeGenerated matches the source
// timestamp instead of being truncated to the second.
const timeGeneratedLayout = "2006-01-02T15:04:05.9999999Z07:00"

func sentinelTimeGenerated(entry *core.LogEntry) (string, error) {
	if !entry.Timestamp.IsZero() {
		return entry.Timestamp.UTC().Format(timeGeneratedLayout), nil
	}
	if ts := entry.Fields["event_time"]; ts != "" {
		if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
			return parsed.UTC().Format(timeGeneratedLayout), nil
		}
		return "", fmt.Errorf("sentinel payload: invalid event_time %q", ts)
	}
	// No timestamp available. Return an error so the caller can
	// route this entry to the DLQ rather than fail the entire batch.
	return "", fmt.Errorf("sentinel payload: timestamp is required")
}

// SentinelTimeGenerated is a public wrapper used to filter out entries
// with missing timestamps before building a payload.
func SentinelTimeGenerated(entry *core.LogEntry) (string, error) {
	return sentinelTimeGenerated(entry)
}
