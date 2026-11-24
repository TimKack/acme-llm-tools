// Copyright © 2026 Fazlul Shahriar. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package acmeclient

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingWriter struct {
	mu     sync.Mutex
	buf    strings.Builder
	writes int
}

func (r *recordingWriter) write(p []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf.Write(p)
	r.writes++
}

func (r *recordingWriter) snapshot() (string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String(), r.writes
}

func TestBodyWriterFlushWritesInOrder(t *testing.T) {
	var rec recordingWriter
	b := newBodyWriter(rec.write)
	defer b.close()

	var want strings.Builder
	for i := 0; i < 5000; i++ {
		s := fmt.Sprintf("chunk %d\n", i)
		want.WriteString(s)
		b.enqueue(s)
	}
	b.flush()

	if got, _ := rec.snapshot(); got != want.String() {
		t.Fatalf("body after flush = %d bytes, want %d bytes in enqueue order", len(got), want.Len())
	}
}

func TestBodyWriterEnqueueDoesNotWaitForSlowWrite(t *testing.T) {
	release := make(chan struct{})
	var rec recordingWriter
	b := newBodyWriter(func(p []byte) {
		<-release
		rec.write(p)
	})

	start := time.Now()
	for i := 0; i < 10000; i++ {
		b.enqueue("x")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("10000 enqueue calls took %v while the write was blocked", d)
	}

	close(release)
	b.flush()
	got, writes := rec.snapshot()
	if got != strings.Repeat("x", 10000) {
		t.Fatalf("body = %d bytes, want 10000", len(got))
	}
	// The first write may take a single chunk; everything queued while it was
	// blocked is coalesced into the next one.
	if writes > 2 {
		t.Fatalf("writes = %d, want at most 2", writes)
	}
	b.close()
}

func TestBodyWriterCloseDrainsPendingOutput(t *testing.T) {
	var rec recordingWriter
	b := newBodyWriter(rec.write)
	b.enqueue("a")
	b.enqueue("b")
	b.close()

	if got, _ := rec.snapshot(); got != "ab" {
		t.Fatalf("body after close = %q, want %q", got, "ab")
	}

	b.enqueue("c")
	b.flush()
	if got, _ := rec.snapshot(); got != "ab" {
		t.Fatalf("body after enqueue on closed writer = %q, want %q", got, "ab")
	}
}

func TestBodyWriterFlushWithNothingQueuedReturns(t *testing.T) {
	var rec recordingWriter
	b := newBodyWriter(rec.write)
	defer b.close()

	b.enqueue("")
	b.flush()
	if got, writes := rec.snapshot(); got != "" || writes != 0 {
		t.Fatalf("body = %q after %d writes, want empty after 0", got, writes)
	}
}
