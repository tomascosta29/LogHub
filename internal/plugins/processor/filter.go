package processor

import (
	"context"
	"regexp"
	"strings"

	"loghub/internal/core"
	"loghub/internal/util"
)

func init() {
	Register("filter", func(props map[string]interface{}) (core.Processor, error) {
		var cfg FilterConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		return NewFilter(cfg)
	})

}

// FilterConfig configures the filter processor.
type FilterConfig struct {
	// IncludeLevel filters to only include logs with these levels (case-insensitive).
	// If empty, all levels are allowed.
	IncludeLevel []string `json:"include_level"`

	// ExcludeLevel excludes logs with these levels (case-insensitive).
	ExcludeLevel []string `json:"exclude_level"`

	// IncludeMessage is a regex pattern; only logs matching it are kept.
	IncludeMessage string `json:"include_message"`

	// ExcludeMessage is a regex pattern; logs matching it are dropped.
	ExcludeMessage string `json:"exclude_message"`

	// IncludeFields requires all specified fields to match exactly.
	IncludeFields map[string]string `json:"include_fields"`

	// ExcludeFields drops logs if all specified fields match exactly.
	ExcludeFields map[string]string `json:"exclude_fields"`
}

// Filter implements a field/level/message based log filter.
type Filter struct {
	config          FilterConfig
	includeMsgRe    *regexp.Regexp
	excludeMsgRe    *regexp.Regexp
	includeLevelSet map[string]struct{}
	excludeLevelSet map[string]struct{}
}

func NewFilter(cfg FilterConfig) (*Filter, error) {
	f := &Filter{
		config:          cfg,
		includeLevelSet: make(map[string]struct{}, len(cfg.IncludeLevel)),
		excludeLevelSet: make(map[string]struct{}, len(cfg.ExcludeLevel)),
	}

	for _, lvl := range cfg.IncludeLevel {
		f.includeLevelSet[strings.ToLower(lvl)] = struct{}{}
	}
	for _, lvl := range cfg.ExcludeLevel {
		f.excludeLevelSet[strings.ToLower(lvl)] = struct{}{}
	}

	if cfg.IncludeMessage != "" {
		re, err := regexp.Compile(cfg.IncludeMessage)
		if err != nil {
			return nil, err
		}
		f.includeMsgRe = re
	}
	if cfg.ExcludeMessage != "" {
		re, err := regexp.Compile(cfg.ExcludeMessage)
		if err != nil {
			return nil, err
		}
		f.excludeMsgRe = re
	}

	return f, nil
}

func (f *Filter) Process(ctx context.Context, logs []*core.LogEntry) ([]*core.LogEntry, error) {
	result := make([]*core.LogEntry, 0, len(logs))
	for _, entry := range logs {
		if f.matches(entry) {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (f *Filter) matches(entry *core.LogEntry) bool {
	// Check include level
	if len(f.includeLevelSet) > 0 {
		if _, ok := f.includeLevelSet[strings.ToLower(entry.Level)]; !ok {
			return false
		}
	}

	// Check exclude level
	if len(f.excludeLevelSet) > 0 {
		if _, ok := f.excludeLevelSet[strings.ToLower(entry.Level)]; ok {
			return false
		}
	}

	// Check include message regex
	if f.includeMsgRe != nil && !f.includeMsgRe.MatchString(entry.Message) {
		return false
	}

	// Check exclude message regex
	if f.excludeMsgRe != nil && f.excludeMsgRe.MatchString(entry.Message) {
		return false
	}

	// Check include fields (all must match)
	for k, v := range f.config.IncludeFields {
		if entry.Fields[k] != v {
			return false
		}
	}

	// Check exclude fields (all must match to drop)
	if len(f.config.ExcludeFields) > 0 {
		allMatch := true
		for k, v := range f.config.ExcludeFields {
			if entry.Fields[k] != v {
				allMatch = false
				break
			}
		}
		if allMatch {
			return false
		}
	}

	return true
}
