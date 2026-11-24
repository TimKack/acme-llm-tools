// Copyright © 2026 Fazlul Shahriar. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package acmeclient

import "sync"

// bodyWriter serializes writes to a window body through a single goroutine.
//
// The ACP connection delivers notifications to SessionUpdate one at a time
// from a bounded queue and drops the connection when that queue fills up.
// Writing each streamed chunk to acme synchronously is slow enough that a
// burst of notifications, such as the transcript replay sent on session/load,
// overflows the queue.  bodyWriter lets SessionUpdate return as soon as the
// text is buffered, and coalesces everything pending into one acme write.
type bodyWriter struct {
	write func([]byte)

	mu      sync.Mutex
	cond    *sync.Cond
	pending []byte
	queued  uint64 // number of enqueue calls accepted
	written uint64 // value of queued covered by the last completed write
	closed  bool
	done    chan struct{}
}

// newBodyWriter starts a bodyWriter that hands buffered text to write.
func newBodyWriter(write func([]byte)) *bodyWriter {
	b := &bodyWriter{write: write, done: make(chan struct{})}
	b.cond = sync.NewCond(&b.mu)
	go b.run()
	return b
}

// enqueue appends s to the pending output without waiting for it to be written.
// It is a no-op after close.
func (b *bodyWriter) enqueue(s string) {
	if s == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.pending = append(b.pending, s...)
	b.queued++
	b.cond.Broadcast()
}

// flush blocks until everything enqueued before the call has been written.
func (b *bodyWriter) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	target := b.queued
	for b.written < target {
		b.cond.Wait()
	}
}

// close writes any pending output and stops the writer goroutine.
func (b *bodyWriter) close() {
	b.mu.Lock()
	b.closed = true
	b.cond.Broadcast()
	b.mu.Unlock()
	<-b.done
}

func (b *bodyWriter) run() {
	defer close(b.done)
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		for len(b.pending) == 0 && !b.closed {
			b.cond.Wait()
		}
		if len(b.pending) == 0 {
			return
		}
		data, seq := b.pending, b.queued
		b.pending = nil
		b.mu.Unlock()
		b.write(data)
		b.mu.Lock()
		b.written = seq
		b.cond.Broadcast()
	}
}
