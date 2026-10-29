package checkpointer

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestCheckpointPipelineIsolationAndClose(t *testing.T) {
	cp, err := NewFileCheckpointer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	if err := cp.Set("slow", "old"); err != nil {
		t.Fatal(err)
	}
	// Hold the same per-pipeline critical section as a slow fsync or
	// compaction. Other pipelines must still initialize, write and read.
	s := cp.pipelineState("slow")
	s.mu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			s.mu.Unlock()
		}
	}()
	slowDone := make(chan error, 1)
	go func() { slowDone <- cp.Set("slow", "new") }()
	fastDone := make(chan error, 1)
	go func() {
		if err := cp.Set("fast", "independent"); err != nil {
			fastDone <- err
			return
		}
		got, err := cp.Get("fast")
		if err == nil && got != "independent" {
			err = errors.New("wrong cursor")
		}
		fastDone <- err
	}()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("one pipeline blocked another's checkpoint")
	}
	select {
	case err := <-slowDone:
		t.Fatalf("same-pipeline write bypassed its lock: %v", err)
	default:
	}
	// Ensure the slow operation acquired a lifecycle read lock before
	// testing that Close waits for it, rather than racing its startup.
	deadline := time.Now().Add(time.Second)
	for cp.lifecycle.TryLock() {
		cp.lifecycle.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("slow operation did not start")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan error, 1)
	go func() { closed <- cp.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close did not wait for active write: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	s.mu.Unlock()
	unlocked = true
	select {
	case err := <-slowDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow write did not finish")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
	if err := cp.Set("fast", "late"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Set after Close: %v", err)
	}
	if _, err := cp.Get("fast"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Get after Close: %v", err)
	}
}
