package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"loghub/internal/bootstrap"
	"loghub/internal/config"
	"loghub/internal/dlq"
)

const dlqUsage = `usage: loghub dlq <list|export> [flags]

  list     summarise each pipeline's dead-letter queue
  export   write the DLQ records as JSON lines to stdout, oldest first

flags:
`

// runDLQ implements `loghub dlq`. It only reads the DLQ files, so it is
// safe to run while LogHub is running.
func runDLQ(args []string, stdout, stderr io.Writer) error {
	fs, opts := newDLQFlags(stderr)
	if len(args) == 0 || (args[0] != "list" && args[0] != "export") {
		fmt.Fprint(stderr, dlqUsage)
		fs.PrintDefaults()
		return errors.New("dlq: expected list or export")
	}
	cmd := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	cfg, err := config.Load(*opts.configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	queues, err := dlqFiles(cfg, *opts.pipelineID, *opts.archives)
	if err != nil {
		return err
	}
	if cmd == "export" {
		return exportDLQ(queues, stdout)
	}
	return listDLQ(queues, stdout)
}

type dlqOptions struct {
	configPath *string
	pipelineID *string
	archives   *bool
}

func newDLQFlags(stderr io.Writer) (*flag.FlagSet, dlqOptions) {
	fs := flag.NewFlagSet("loghub dlq", flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs, dlqOptions{
		configPath: fs.String("config", "loghub.yaml", "Path to configuration file"),
		pipelineID: fs.String("pipeline", "", "Only this pipeline (default: every pipeline with a DLQ)"),
		archives:   fs.Bool("archives", false, "Include rotated DLQ archives"),
	}
}

type dlqQueue struct {
	pipelineID string
	paths      []string // oldest first; the current journal last
}

// dlqFiles returns the DLQ journal files for the selected pipelines.
func dlqFiles(cfg *config.Config, pipelineID string, withArchives bool) ([]dlqQueue, error) {
	var out []dlqQueue
	for i := range cfg.Pipelines {
		pc := &cfg.Pipelines[i]
		if pipelineID != "" && pc.ID != pipelineID {
			continue
		}
		if !pc.Durability.DLQ.Enabled {
			if pipelineID != "" {
				return nil, fmt.Errorf("pipeline %s has no DLQ enabled", pipelineID)
			}
			continue
		}
		dir, err := bootstrap.DLQDir(&cfg.Global, pc)
		if err != nil {
			return nil, err
		}
		var paths []string
		if withArchives {
			if paths, err = dlq.ArchivePaths(dir, pc.ID); err != nil {
				return nil, err
			}
		}
		out = append(out, dlqQueue{pipelineID: pc.ID, paths: append(paths, dlq.JournalPath(dir, pc.ID))})
	}
	if pipelineID != "" && len(out) == 0 {
		return nil, fmt.Errorf("pipeline %s is not in the configuration", pipelineID)
	}
	return out, nil
}

func exportDLQ(queues []dlqQueue, w io.Writer) error {
	enc := json.NewEncoder(w)
	for _, q := range queues {
		for _, path := range q.paths {
			if err := dlq.ReadFile(path, func(fb dlq.FailedBatch) error { return enc.Encode(fb) }); err != nil {
				return fmt.Errorf("read %s: %w", path, err)
			}
		}
	}
	return nil
}

func listDLQ(queues []dlqQueue, w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PIPELINE\tDESTINATION\tRECORDS\tENTRIES\tOLDEST\tNEWEST")
	for _, q := range queues {
		type summary struct {
			records, entries int
			oldest, newest   time.Time
		}
		byDest := map[string]*summary{}
		for _, path := range q.paths {
			err := dlq.ReadFile(path, func(fb dlq.FailedBatch) error {
				s := byDest[fb.Destination]
				if s == nil {
					s = &summary{oldest: fb.Timestamp, newest: fb.Timestamp}
					byDest[fb.Destination] = s
				}
				s.records++
				s.entries += len(fb.Entries)
				if fb.Timestamp.Before(s.oldest) {
					s.oldest = fb.Timestamp
				}
				if fb.Timestamp.After(s.newest) {
					s.newest = fb.Timestamp
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("read %s: %w", path, err)
			}
		}
		if len(byDest) == 0 {
			fmt.Fprintf(tw, "%s\t-\t0\t0\t-\t-\n", q.pipelineID)
			continue
		}
		dests := make([]string, 0, len(byDest))
		for d := range byDest {
			dests = append(dests, d)
		}
		sort.Strings(dests)
		for _, d := range dests {
			s := byDest[d]
			fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\n", q.pipelineID, d, s.records, s.entries,
				s.oldest.Format(time.RFC3339), s.newest.Format(time.RFC3339))
		}
	}
	return tw.Flush()
}
