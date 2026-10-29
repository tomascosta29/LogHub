package processor

import (
	"context"
	"encoding/json"
	"fmt"

	"loghub/internal/core"
	"loghub/internal/util"
)

func init() {
	Register("json_parse", func(props map[string]interface{}) (core.Processor, error) {
		var cfg JSONParseConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		return NewJSONParse(cfg)
	})

}

// JSONParseConfig configures the JSON parse processor.
type JSONParseConfig struct {
	// SourceField is the field to parse as JSON. If empty, uses Message.
	SourceField string `json:"source_field"`

	// TargetField selects where parsed JSON goes: "fields" (or empty)
	// merges it into the top-level Fields map, and "message" also sets
	// Message from the parsed "message" key. Other values are rejected.
	TargetField string `json:"target_field"`

	// KeepSource whether to keep the original source field after parsing.
	KeepSource bool `json:"keep_source"`

	// OnError defines behavior when parsing fails: "drop", "keep", or "poison" (default: keep).
	OnError string `json:"on_error"`
}

// JSONParse parses JSON content from log entries and merges it into structured fields.
type JSONParse struct {
	config JSONParseConfig
}

func NewJSONParse(cfg JSONParseConfig) (*JSONParse, error) {
	switch cfg.TargetField {
	case "", "fields", "message":
	default:
		return nil, fmt.Errorf("json_parse: target_field must be \"fields\" or \"message\", got %q", cfg.TargetField)
	}
	if cfg.OnError == "" {
		cfg.OnError = string(ErrorModeKeep)
	}
	return &JSONParse{config: cfg}, nil
}

func (j *JSONParse) Process(ctx context.Context, logs []*core.LogEntry) ([]*core.LogEntry, error) {
	return processBatch(ctx, logs, ErrorMode(j.config.OnError), "json_parse", j.processEntry)
}

func (j *JSONParse) processEntry(entry *core.LogEntry) (*core.LogEntry, error) {
	source := entry.Message
	if j.config.SourceField != "" {
		if v, ok := entry.Fields[j.config.SourceField]; ok {
			source = v
		} else {
			return entry, nil // field not found, nothing to do
		}
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(source), &parsed); err != nil {
		return nil, err
	}

	newEntry := core.CopyLogEntry(entry)

	switch j.config.TargetField {
	case "message":
		if msg, ok := parsed["message"].(string); ok {
			newEntry.Message = msg
		}
		for k, v := range parsed {
			if k == "message" {
				continue
			}
			setField(newEntry, k, v)
		}
	default:
		for k, v := range parsed {
			setField(newEntry, k, v)
		}
	}

	if !j.config.KeepSource && j.config.SourceField != "" {
		delete(newEntry.Fields, j.config.SourceField)
	}

	return newEntry, nil
}

func setField(entry *core.LogEntry, key string, value interface{}) {
	if s, ok := value.(string); ok {
		entry.Fields[key] = s
	} else {
		entry.Fields[key] = fmt.Sprintf("%v", value)
	}
}
