package registry

import (
	"fmt"
	"sync"
	"testing"
)

func TestRegistryRegisterLookup(t *testing.T) {
	r := New[func() int]()
	if _, ok := r.Lookup("a"); ok {
		t.Fatal("Lookup on empty registry: ok = true")
	}
	r.Register("a", func() int { return 1 })
	r.Register("a", func() int { return 2 })
	fn, ok := r.Lookup("a")
	if !ok || fn() != 2 {
		t.Errorf("Lookup(a) = %v, want the later registration", ok)
	}
	if r.IsBuiltIn("a") {
		t.Error("IsBuiltIn(a) = true before RegisterBuiltIn")
	}
	r.RegisterBuiltIn("a")
	if !r.IsBuiltIn("a") {
		t.Error("IsBuiltIn(a) = false after RegisterBuiltIn")
	}
}

// Run with -race: registering while other goroutines look up must not
// race.
func TestRegistryConcurrentUse(t *testing.T) {
	r := New[int]()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Register(fmt.Sprintf("p%d-%d", i, j), j)
				r.RegisterBuiltIn(fmt.Sprintf("p%d-%d", i, j))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Lookup(fmt.Sprintf("p%d-%d", i, j))
				r.IsBuiltIn(fmt.Sprintf("p%d-%d", i, j))
			}
		}()
	}
	wg.Wait()
}
