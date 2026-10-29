package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	EntriesCollected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_entries_collected_total",
			Help: "Total number of log entries collected from sources",
		},
		[]string{"pipeline_id", "source_name"},
	)

	EntriesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_entries_sent_total",
			Help: "Total number of log entries sent to destinations",
		},
		[]string{"pipeline_id", "destination_type"},
	)

	EntriesFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_entries_failed_total",
			Help: "Total number of log entries that failed to send",
		},
		[]string{"pipeline_id", "destination_type"},
	)

	BatchesCollected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_batches_collected_total",
			Help: "Total number of batches collected from sources",
		},
		[]string{"pipeline_id"},
	)

	BatchesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_batches_sent_total",
			Help: "Total number of batches sent to destinations",
		},
		[]string{"pipeline_id"},
	)

	BatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loghub_batch_size",
			Help:    "Size of batches processed",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"pipeline_id"},
	)

	CollectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loghub_collect_duration_seconds",
			Help:    "Time spent collecting from sources",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"pipeline_id"},
	)

	SendDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loghub_send_duration_seconds",
			Help:    "Time spent sending to destinations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"pipeline_id"},
	)

	Retries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_retries_total",
			Help: "Total number of retry attempts",
		},
		[]string{"pipeline_id", "destination_type"},
	)

	CheckpointsSaved = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_checkpoints_saved_total",
			Help: "Total number of checkpoints saved",
		},
		[]string{"pipeline_id"},
	)

	PipelineRunning = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loghub_pipeline_running",
			Help: "Whether a pipeline is currently running (1) or stopped (0)",
		},
		[]string{"pipeline_id"},
	)

	// PipelineStatus reports the current PipelineStatus of each pipeline
	// (stopped / starting / running / degraded / stalled / draining /
	// stopping). The status label is set to the current value and the
	// previous one is reset to 0 on transition, so the alertable query
	// `loghub_pipeline_status == 1` yields the active state per
	// pipeline_id.
	PipelineStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loghub_pipeline_status",
			Help: "Current pipeline state (1 for the active state per pipeline, 0 for the others)",
		},
		[]string{"pipeline_id", "status"},
	)

	PollInterval = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loghub_poll_interval_seconds",
			Help: "Configured poll interval for each pipeline",
		},
		[]string{"pipeline_id"},
	)

	BufferUtilization = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loghub_buffer_utilization",
			Help: "Current buffer utilization (number of pending batches)",
		},
		[]string{"pipeline_id"},
	)

	DiskBufferBatches = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loghub_disk_buffer_batches",
			Help: "Number of batches currently stored in disk buffer",
		},
		[]string{"pipeline_id"},
	)

	EntriesDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_entries_dropped_total",
			Help: "Total number of log entries dropped because no DLQ or disk buffer could hold them",
		},
		[]string{"pipeline_id", "reason"},
	)

	DLQEntries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_dlq_entries_total",
			Help: "Total number of entries written to dead letter queue",
		},
		[]string{"pipeline_id"},
	)

	// CircuitBreakerState reports the current per-destination
	// circuit breaker state (0=closed, 1=open, 2=half-open). The
	// destination_type label matches the per-destination labels used
	// elsewhere so operators can correlate tripped state with
	// destination errors.
	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loghub_circuit_breaker_state",
			Help: "Current circuit breaker state per (pipeline, destination): 0=closed, 1=open, 2=half-open",
		},
		[]string{"pipeline_id", "destination_type"},
	)

	// CircuitBreakerTrippedTotal counts the number of times a
	// circuit breaker has transitioned from closed (or half-open) to
	// open. Useful for alerting on destination instability.
	CircuitBreakerTrippedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_circuit_breaker_tripped_total",
			Help: "Total number of times a circuit breaker tripped open (or re-opened from half-open)",
		},
		[]string{"pipeline_id", "destination_type"},
	)

	// SecretRefreshTotal counts secret lookups by the secret cache (on
	// first use, on reload, and in the background refresh), per provider.
	SecretRefreshTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_secret_refresh_total",
			Help: "Total number of secret lookups by the secret cache",
		},
		[]string{"provider"},
	)

	// SecretRefreshErrorsTotal counts failed secret lookups. A failure
	// with a previously resolved value keeps serving that value.
	SecretRefreshErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loghub_secret_refresh_errors_total",
			Help: "Total number of failed secret lookups by the secret cache",
		},
		[]string{"provider"},
	)
)

func Register() {
	prometheus.MustRegister(
		EntriesCollected,
		EntriesSent,
		EntriesFailed,
		BatchesCollected,
		BatchesSent,
		BatchSize,
		CollectDuration,
		SendDuration,
		Retries,
		CheckpointsSaved,
		PipelineRunning,
		PipelineStatus,
		PollInterval,
		BufferUtilization,
		DiskBufferBatches,
		DLQEntries,
		EntriesDropped,
		CircuitBreakerState,
		CircuitBreakerTrippedTotal,
		SecretRefreshTotal,
		SecretRefreshErrorsTotal,
	)
}

func Handler() http.Handler {
	return promhttp.Handler()
}

// Trace is a minimal tracing interface. The default
// implementation is a no-op; production deployments can supply an
// OpenTelemetry-backed implementation. The interface is intentionally
// minimal so it does not require an OpenTelemetry dependency at the
// call sites.
type Trace interface {
	// Start begins a span and returns a function to end it. The returned
	// function records the span as completed; multiple calls are safe
	// (the second call is a no-op).
	Start(name string) func(err *error)
}

// NopTrace is the default no-op tracer.
type NopTrace struct{}

func (NopTrace) Start(name string) func(err *error) {
	return func(*error) {}
}

// globalTrace is the package-global tracer. Set via SetTracer; defaults
// to NopTrace.
var globalTrace Trace = NopTrace{}

// SetTracer installs a process-wide tracer.
func SetTracer(t Trace) {
	if t == nil {
		globalTrace = NopTrace{}
		return
	}
	globalTrace = t
}

// Span begins a span and returns a function to end it.
func Span(name string) func(err *error) {
	return globalTrace.Start(name)
}
