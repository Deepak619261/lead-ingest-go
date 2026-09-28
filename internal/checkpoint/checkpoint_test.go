package checkpoint

import (
	"path/filepath"
	"sync"
	"testing"
)

func TestWatermarkAdvancesOnlyWhenContiguous(t *testing.T) {
	tr := NewTracker(0)
	tr.Done(2, 3)
	if w := tr.Watermark(); w != 0 {
		t.Fatalf("w=%d, want 0 (row 1 not done yet)", w)
	}
	tr.Done(1)
	if w := tr.Watermark(); w != 3 {
		t.Fatalf("w=%d, want 3", w)
	}
	tr.Done(5)
	if w := tr.Watermark(); w != 3 {
		t.Fatalf("w=%d, want 3 (gap at 4)", w)
	}
}

func TestResumeStart(t *testing.T) {
	tr := NewTracker(100)
	tr.Done(50, 101) // 50 is already below the start: ignored
	if w := tr.Watermark(); w != 101 {
		t.Fatalf("w=%d", w)
	}
}

func TestConcurrentDone(t *testing.T) {
	tr := NewTracker(0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for r := g + 1; r <= 10_000; r += 8 {
				tr.Done(r)
			}
		}(g)
	}
	wg.Wait()
	if w := tr.Watermark(); w != 10_000 {
		t.Fatalf("w=%d", w)
	}
}

func TestSaveLoad(t *testing.T) {
	p := filepath.Join(t.TempDir(), "imp.ckpt")
	if s, err := Load(p); err != nil || s.Watermark != 0 {
		t.Fatalf("missing file: %+v %v", s, err)
	}
	want := State{ImportID: "imp-1", AccountID: 7, Watermark: 1234}
	if err := Save(p, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(p)
	if err != nil || got != want {
		t.Fatalf("got %+v %v", got, err)
	}
}
