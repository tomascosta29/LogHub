package lock

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestAcquireAndRelease(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock is not supported on Windows in this implementation; production target is Linux")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if l.Path() != path {
		t.Errorf("Path() = %q, want %q", l.Path(), path)
	}
	if err := l.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

func TestAcquireFailsWhenHeldByOtherProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock is not supported on Windows in this implementation; production target is Linux")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer first.Release()

	_, err = Acquire(path)
	if err == nil {
		t.Fatal("expected second Acquire to fail while first is held")
	}
	if !errors.Is(err, errAlreadyLocked) {
		t.Errorf("expected errAlreadyLocked, got: %v", err)
	}
}

func TestReleaseAllowsReacquire(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock is not supported on Windows in this implementation; production target is Linux")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second, err := Acquire(path)
	if err != nil {
		t.Fatalf("second Acquire after Release: %v", err)
	}
	defer second.Release()
}

func TestReleaseIsIdempotent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock is not supported on Windows in this implementation; production target is Linux")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Errorf("second Release should be no-op, got: %v", err)
	}
}

func TestAcquireCreatesParentDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock is not supported on Windows in this implementation; production target is Linux")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "subdir", "test.lock")

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire with nested path: %v", err)
	}
	defer l.Release()

	if _, err := os.Stat(path); err != nil {
		t.Errorf("expected lock file to exist: %v", err)
	}
}

func TestTryAcquireFailsWhenFileMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing.lock")
	if _, err := TryAcquire(path); err == nil {
		t.Error("expected TryAcquire to fail when file does not exist")
	}
}
