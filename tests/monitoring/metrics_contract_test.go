package monitoring_test

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"

	"loghub/internal/metrics"
)

// metricSpec is one metric definition found in internal/metrics/metrics.go.
type metricSpec struct {
	Name   string
	Type   string // counter, gauge, histogram
	Labels []string
	Var    string
}

var (
	metricDocRow = regexp.MustCompile(`^\|\s*` + "`" + `(loghub_[a-z0-9_]+)` + "`" + `\s*\|\s*(.*?)\s*\|`)
	docLabel     = regexp.MustCompile("`([a-z_][a-z0-9_]*)`")
	metricToken  = regexp.MustCompile(`\bloghub_[a-z0-9_]+\b`)
	alertLine    = regexp.MustCompile(`^\s{6,}- alert:`)
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}

func readDoc(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "monitoring.md"))
	if err != nil {
		t.Fatalf("read monitoring docs: %v", err)
	}
	return string(data)
}

// parseMetricsSource extracts every Prometheus vector defined in
// internal/metrics/metrics.go. Reading the source (rather than the
// registry) also sees metrics that have no samples yet.
func parseMetricsSource(t *testing.T) map[string]metricSpec {
	t.Helper()
	path := filepath.Join(repoRoot(t), "internal", "metrics", "metrics.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	specs := make(map[string]metricSpec)
	ast.Inspect(file, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
			return true
		}
		call, ok := vs.Values[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "prometheus" {
			return true
		}
		var typ string
		switch sel.Sel.Name {
		case "NewCounterVec":
			typ = "counter"
		case "NewGaugeVec":
			typ = "gauge"
		case "NewHistogramVec":
			typ = "histogram"
		default:
			return true
		}
		var name string
		opts, ok := call.Args[0].(*ast.CompositeLit)
		if !ok {
			return true
		}
		for _, elt := range opts.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Name" {
				name = stringLiteral(kv.Value)
			}
		}
		if name == "" {
			t.Errorf("%s: metric %s has no Name", filepath.Base(path), vs.Names[0].Name)
			return true
		}
		var labels []string
		if len(call.Args) > 1 {
			lit, ok := call.Args[1].(*ast.CompositeLit)
			if ok {
				for _, elt := range lit.Elts {
					if label := stringLiteral(elt); label != "" {
						labels = append(labels, label)
					}
				}
			}
		}
		specs[vs.Names[0].Name] = metricSpec{Name: name, Type: typ, Labels: labels, Var: vs.Names[0].Name}
		return true
	})
	if len(specs) == 0 {
		t.Fatal("no Prometheus metrics found in internal/metrics/metrics.go")
	}
	return specs
}

func stringLiteral(n ast.Node) string {
	lit, ok := n.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	s := strings.Trim(lit.Value, "`\"")
	return s
}

// parseDocMetrics extracts the metric tables from docs/monitoring.md.
func parseDocMetrics(t *testing.T, doc string) map[string]metricSpec {
	t.Helper()
	specs := make(map[string]metricSpec)
	section := ""
	scanner := bufio.NewScanner(strings.NewReader(doc))
	for scanner.Scan() {
		line := scanner.Text()
		switch strings.TrimSpace(line) {
		case "### Counters":
			section = "counter"
		case "### Gauges":
			section = "gauge"
		case "### Histograms":
			section = "histogram"
		}
		m := metricDocRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if section == "" {
			t.Errorf("metric %s documented outside a Counters/Gauges/Histograms section", m[1])
			continue
		}
		if _, dup := specs[m[1]]; dup {
			t.Errorf("metric %s documented more than once", m[1])
			continue
		}
		var labels []string
		for _, lm := range docLabel.FindAllStringSubmatch(m[2], -1) {
			labels = append(labels, lm[1])
		}
		specs[m[1]] = metricSpec{Name: m[1], Type: section, Labels: labels}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan monitoring docs: %v", err)
	}
	if len(specs) == 0 {
		t.Fatal("no metric tables found in docs/monitoring.md")
	}
	return specs
}

func TestMonitoringDocsMatchMetricSource(t *testing.T) {
	source := parseMetricsSource(t)
	docs := parseDocMetrics(t, readDoc(t))

	byName := make(map[string]metricSpec, len(source))
	for _, spec := range source {
		byName[spec.Name] = spec
	}

	for _, spec := range source {
		docSpec, ok := docs[spec.Name]
		if !ok {
			t.Errorf("metric %s (%s) is defined in internal/metrics/metrics.go but is missing from docs/monitoring.md", spec.Name, spec.Var)
			continue
		}
		if docSpec.Type != spec.Type {
			t.Errorf("metric %s: docs say %s, source defines %s", spec.Name, docSpec.Type, spec.Type)
		}
		if !sameStrings(docSpec.Labels, spec.Labels) {
			t.Errorf("metric %s: docs labels %v, source labels %v", spec.Name, docSpec.Labels, spec.Labels)
		}
	}
	for name := range docs {
		if _, ok := byName[name]; !ok {
			t.Errorf("docs/monitoring.md documents %s but internal/metrics/metrics.go does not define it", name)
		}
	}
}

func TestEveryMetricIsRegistered(t *testing.T) {
	path := filepath.Join(repoRoot(t), "internal", "metrics", "metrics.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	source := parseMetricsSource(t)
	registered := make(map[string]bool)
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Register" || fn.Body == nil {
			return true
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "prometheus" || sel.Sel.Name != "MustRegister" {
				return true
			}
			for _, arg := range call.Args {
				if id, ok := arg.(*ast.Ident); ok {
					registered[id.Name] = true
				}
			}
			return true
		})
		return false
	})

	for varName, spec := range source {
		if !registered[varName] {
			t.Errorf("metric %s (%s) is defined but not passed to prometheus.MustRegister in Register()", spec.Name, varName)
		}
	}
	for varName := range registered {
		if _, ok := source[varName]; !ok {
			t.Errorf("Register() references %s, which is not a metric defined in this file", varName)
		}
	}
}

func TestMetricsHandlerMatchesDocs(t *testing.T) {
	docs := parseDocMetrics(t, readDoc(t))
	touchAllMetrics(t, docs)

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("metrics handler status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("metrics Content-Type = %q, want text/plain", ct)
	}

	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatalf("parse metrics output: %v", err)
	}

	for _, docSpec := range docs {
		fam, ok := families[docSpec.Name]
		if !ok {
			t.Errorf("documented metric %s is not exposed by /metrics", docSpec.Name)
			continue
		}
		if got := strings.ToLower(fam.GetType().String()); got != docSpec.Type {
			t.Errorf("metric %s: /metrics type %s, docs say %s", docSpec.Name, got, docSpec.Type)
			continue
		}
		if len(fam.GetMetric()) == 0 {
			t.Errorf("metric %s has no sample after touch", docSpec.Name)
			continue
		}
		labels := make([]string, 0, len(fam.GetMetric()[0].GetLabel()))
		for _, lp := range fam.GetMetric()[0].GetLabel() {
			labels = append(labels, lp.GetName())
		}
		if !sameStrings(labels, docSpec.Labels) {
			t.Errorf("metric %s: /metrics labels %v, docs labels %v", docSpec.Name, labels, docSpec.Labels)
		}
	}

	for name := range families {
		if !strings.HasPrefix(name, "loghub_") {
			continue
		}
		if _, ok := docs[name]; !ok {
			t.Errorf("metric %s is exposed by /metrics but not documented in docs/monitoring.md", name)
		}
	}
}

// touchAllMetrics creates one sample for every documented metric so empty
// vectors are included in the gathered output. The map is deliberately
// exhaustive: adding a documented metric without adding it here fails
// the test.
func touchAllMetrics(t *testing.T, docs map[string]metricSpec) {
	t.Helper()
	touch := map[string]func(){
		"loghub_entries_collected_total":       func() { metrics.EntriesCollected.WithLabelValues("test", "source").Add(0) },
		"loghub_entries_sent_total":            func() { metrics.EntriesSent.WithLabelValues("test", "http").Add(0) },
		"loghub_entries_failed_total":          func() { metrics.EntriesFailed.WithLabelValues("test", "http").Add(0) },
		"loghub_collect_errors_total":          func() { metrics.CollectErrors.WithLabelValues("test", "test-source").Add(0) },
		"loghub_batches_collected_total":       func() { metrics.BatchesCollected.WithLabelValues("test").Add(0) },
		"loghub_batches_sent_total":            func() { metrics.BatchesSent.WithLabelValues("test").Add(0) },
		"loghub_retries_total":                 func() { metrics.Retries.WithLabelValues("test", "http").Add(0) },
		"loghub_checkpoints_saved_total":       func() { metrics.CheckpointsSaved.WithLabelValues("test").Add(0) },
		"loghub_dlq_entries_total":             func() { metrics.DLQEntries.WithLabelValues("test").Add(0) },
		"loghub_entries_dropped_total":         func() { metrics.EntriesDropped.WithLabelValues("test", "destination_rejected").Add(0) },
		"loghub_circuit_breaker_tripped_total": func() { metrics.CircuitBreakerTrippedTotal.WithLabelValues("test", "http").Add(0) },
		"loghub_secret_refresh_total":          func() { metrics.SecretRefreshTotal.WithLabelValues("env").Add(0) },
		"loghub_secret_refresh_errors_total":   func() { metrics.SecretRefreshErrorsTotal.WithLabelValues("env").Add(0) },
		"loghub_pipeline_running":              func() { metrics.PipelineRunning.WithLabelValues("test").Set(0) },
		"loghub_pipeline_status":               func() { metrics.PipelineStatus.WithLabelValues("test", "running").Set(0) },
		"loghub_poll_interval_seconds":         func() { metrics.PollInterval.WithLabelValues("test").Set(0) },
		"loghub_buffer_utilization":            func() { metrics.BufferUtilization.WithLabelValues("test").Set(0) },
		"loghub_disk_buffer_batches":           func() { metrics.DiskBufferBatches.WithLabelValues("test").Set(0) },
		"loghub_circuit_breaker_state":         func() { metrics.CircuitBreakerState.WithLabelValues("test", "http").Set(0) },
		"loghub_batch_size":                    func() { metrics.BatchSize.WithLabelValues("test").Observe(0) },
		"loghub_collect_duration_seconds":      func() { metrics.CollectDuration.WithLabelValues("test").Observe(0) },
		"loghub_send_duration_seconds":         func() { metrics.SendDuration.WithLabelValues("test").Observe(0) },
	}
	for name := range docs {
		fn, ok := touch[name]
		if !ok {
			t.Fatalf("no touch function for documented metric %s; add one to touchAllMetrics", name)
		}
		fn()
	}
}

type alertRulesFile struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Alert string `yaml:"alert"`
			Expr  string `yaml:"expr"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

// TestAlertRulesFromDocsAreValid assembles the alert rule snippets from
// docs/monitoring.md into one rule group and validates them. The docs
// present the rules as several fragments to paste into an existing group,
// so this checks the fragments still combine into valid YAML.
func TestAlertRulesFromDocsAreValid(t *testing.T) {
	doc := readDoc(t)
	rulesYAML := assembleAlertRules(t, doc)

	var parsed alertRulesFile
	if err := yaml.Unmarshal([]byte(rulesYAML), &parsed); err != nil {
		t.Fatalf("alert rules from docs/monitoring.md are not valid YAML: %v\n%s", err, rulesYAML)
	}
	if len(parsed.Groups) != 1 {
		t.Fatalf("expected 1 assembled rule group, got %d", len(parsed.Groups))
	}
	rules := parsed.Groups[0].Rules
	if len(rules) < 15 {
		t.Fatalf("expected at least 15 alert rules in docs/monitoring.md, got %d", len(rules))
	}

	docs := parseDocMetrics(t, doc)
	known := make(map[string]bool, len(docs))
	for name := range docs {
		known[name] = true
	}
	seen := make(map[string]bool)
	for _, rule := range rules {
		if rule.Alert == "" {
			t.Error("alert rule without an alert name")
			continue
		}
		if seen[rule.Alert] {
			t.Errorf("duplicate alert name %s", rule.Alert)
		}
		seen[rule.Alert] = true
		if strings.TrimSpace(rule.Expr) == "" {
			t.Errorf("alert %s has an empty expr", rule.Alert)
			continue
		}
		for _, token := range metricToken.FindAllString(rule.Expr, -1) {
			base := normalizeMetricToken(token, known)
			if base == "" {
				t.Errorf("alert %s references unknown metric %s", rule.Alert, token)
			}
		}
	}
}

// TestMonitoringDocsMentionOnlyKnownMetrics catches typos and stale metric
// names anywhere in the monitoring guide, not just in alert expressions.
func TestMonitoringDocsMentionOnlyKnownMetrics(t *testing.T) {
	doc := readDoc(t)
	docs := parseDocMetrics(t, doc)
	known := make(map[string]bool, len(docs))
	for name := range docs {
		known[name] = true
	}
	for _, token := range metricToken.FindAllString(doc, -1) {
		if normalizeMetricToken(token, known) == "" {
			t.Errorf("docs/monitoring.md mentions unknown metric %s", token)
		}
	}
}

// normalizeMetricToken accepts a family name or one of the derived series
// suffixes that Prometheus generates for histograms.
func normalizeMetricToken(token string, known map[string]bool) string {
	if known[token] {
		return token
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		if strings.HasSuffix(token, suffix) {
			base := strings.TrimSuffix(token, suffix)
			if known[base] {
				return base
			}
		}
	}
	return ""
}

func assembleAlertRules(t *testing.T, doc string) string {
	t.Helper()
	start := strings.Index(doc, "## Alerting Rules")
	end := strings.Index(doc, "## AlertManager Configuration")
	if start < 0 || end < 0 || end <= start {
		t.Fatal("cannot locate the Alerting Rules section in docs/monitoring.md")
	}
	section := doc[start:end]

	var blocks []string
	var current strings.Builder
	inFence := false
	scanner := bufio.NewScanner(strings.NewReader(section))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			if inFence {
				blocks = append(blocks, current.String())
				current.Reset()
			}
			inFence = !inFence
			continue
		}
		if inFence {
			current.WriteString(line)
			current.WriteByte('\n')
		}
	}
	if inFence {
		t.Fatal("unterminated code fence in the Alerting Rules section")
	}
	if len(blocks) == 0 {
		t.Fatal("no alert rule code blocks found in docs/monitoring.md")
	}

	var out strings.Builder
	out.WriteString("groups:\n  - name: loghub-docs\n    rules:\n")
	ruleLines := 0
	for _, block := range blocks {
		for _, line := range strings.Split(block, "\n") {
			if !alertLine.MatchString(line) && !strings.HasPrefix(line, "        ") {
				continue
			}
			out.WriteString(line)
			out.WriteByte('\n')
			if alertLine.MatchString(line) {
				ruleLines++
			}
		}
	}
	if ruleLines == 0 {
		t.Fatal("assembled alert rule group has no rules")
	}
	return out.String()
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
