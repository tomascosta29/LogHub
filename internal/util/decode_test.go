package util

import "testing"

type jsonTaggedConfig struct {
	IncludeLevel []string `json:"include_level"`
	SourceField  string   `json:"source_field"`
}

func TestDecodePropertiesUsesJSONTags(t *testing.T) {
	var cfg jsonTaggedConfig
	err := DecodeProperties(map[string]interface{}{
		"include_level": []interface{}{"ERROR", "WARN"},
		"source_field":  "payload",
	}, &cfg)
	if err != nil {
		t.Fatalf("DecodeProperties failed: %v", err)
	}

	if len(cfg.IncludeLevel) != 2 || cfg.IncludeLevel[0] != "ERROR" || cfg.SourceField != "payload" {
		t.Fatalf("json-tagged properties were not decoded: %+v", cfg)
	}
}
