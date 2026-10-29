package destination

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"loghub/internal/core"
	"loghub/internal/util"
)

type dummyConfig struct {
	DropRate    float64 `mapstructure:"drop_rate"`
	LogMessages bool    `mapstructure:"log_messages"`
	Latency     string  `mapstructure:"latency"`
}

func init() {
	Register("dummy", func(props map[string]interface{}) (core.Destination, error) {
		var cfg dummyConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		return NewDummy(DummyConfig{
			DropRate:    cfg.DropRate,
			LogMessages: cfg.LogMessages,
			Latency:     cfg.Latency,
		})
	})

}

type Dummy struct {
	dropRate      float64
	logMessages   bool
	receivedCount atomic.Int64
	bytesReceived atomic.Int64
	latency       time.Duration
}

type DummyConfig struct {
	DropRate    float64 `yaml:"drop_rate"`
	LogMessages bool    `yaml:"log_messages"`
	Latency     string  `yaml:"latency"`
}

// errDummyDrop is the rejection the dummy destination reports for the
// entries drop_rate selects.
var errDummyDrop = errors.New("dummy: entry dropped by drop_rate")

func NewDummy(cfg DummyConfig) (*Dummy, error) {
	if cfg.DropRate < 0 || cfg.DropRate > 1 {
		return nil, fmt.Errorf("dummy: drop_rate must be between 0 and 1, got %v", cfg.DropRate)
	}
	d := &Dummy{
		dropRate:    cfg.DropRate,
		logMessages: cfg.LogMessages,
	}

	if cfg.Latency != "" {
		var err error
		d.latency, err = time.ParseDuration(cfg.Latency)
		if err != nil {
			slog.Warn("Invalid latency duration, using 0", "latency", cfg.Latency, "error", err)
			d.latency = 0
		} else if d.latency < 0 {
			d.latency = 0
		}
	}

	return d, nil
}

// Send accepts every entry, after the configured latency, except a
// drop_rate fraction of them, chosen at random, which it rejects with a
// per-entry error as a destination rejecting bad records would.
func (d *Dummy) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	if d.latency > 0 {
		timer := time.NewTimer(d.latency)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}

	statuses := allSuccess(len(logs))
	for i, entry := range logs {
		if d.dropRate > 0 && rand.Float64() < d.dropRate {
			statuses[i].Error = errDummyDrop
			continue
		}
		d.receivedCount.Add(1)
		d.bytesReceived.Add(int64(len(entry.Message)))

		if d.logMessages {
			slog.Info("[Dummy] Received",
				"source", entry.Source,
				"timestamp", entry.Timestamp.Format(time.RFC3339),
				"message", entry.Message)
		}
	}

	return statuses, nil
}

func (d *Dummy) Close() error {
	return nil
}

func (d *Dummy) Type() string {
	return "dummy"
}

func (d *Dummy) Stats() (count int64, bytes int64) {
	return d.receivedCount.Load(), d.bytesReceived.Load()
}
