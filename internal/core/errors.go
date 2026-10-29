package core

import "errors"

// ErrPoisonPill marks an error as a permanent per-entry rejection.
// A destination that returns this (or wraps it) signals to the
// pipeline that the entry will never succeed and the retry loop
// should be skipped. Callers use `errors.Is(err, ErrPoisonPill)`
// to detect this case.
var ErrPoisonPill = errors.New("poison pill: permanently rejected log")
