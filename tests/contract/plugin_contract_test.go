package contract_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"loghub/internal/plugins/destination"
	"loghub/internal/plugins/processor"
	"loghub/internal/plugins/source"
)

// TestEveryBuiltInSchemaHasAFactory guards against advertising a plugin in
// the strict config schema (and RegisterBuiltIn list) without a factory.
func TestEveryBuiltInSchemaHasAFactory(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "schemas", "schemas.go"))
	if err != nil {
		t.Fatalf("read schemas.go: %v", err)
	}
	text := string(data)
	schemaRE := regexp.MustCompile(`RegisterPluginSchema\("([a-z_]+)"`)
	var schemaNames []string
	for _, m := range schemaRE.FindAllStringSubmatch(text, -1) {
		schemaNames = append(schemaNames, m[1])
	}
	if len(schemaNames) == 0 {
		t.Fatal("no plugin schemas found")
	}

	builtInRE := regexp.MustCompile(`(?s)for _, name := range \[\]string\{([^}]*)\}\s*\{\s*(\w+)\.RegisterBuiltIn`)
	categories := make(map[string]string)
	for _, m := range builtInRE.FindAllStringSubmatch(text, -1) {
		for _, q := range regexp.MustCompile(`"([a-z_]+)"`).FindAllStringSubmatch(m[1], -1) {
			categories[q[1]] = m[2]
		}
	}
	if len(categories) == 0 {
		t.Fatal("no RegisterBuiltIn lists found")
	}

	for _, name := range schemaNames {
		category := categories[name]
		var createErr error
		switch category {
		case "source":
			_, createErr = source.Create(name, context.Background(), map[string]interface{}{}, nil)
		case "destination":
			_, createErr = destination.Create(name, map[string]interface{}{})
		case "processor":
			_, createErr = processor.Create(name, map[string]interface{}{})
		default:
			t.Errorf("schema %s is not marked built-in in any registry", name)
			continue
		}
		if createErr != nil && strings.Contains(createErr.Error(), "unknown ") {
			t.Errorf("%s %q is registered as a built-in schema but has no factory: %v", category, name, createErr)
		}
	}
}
