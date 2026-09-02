/**
 *  @file      ringbuffer.go
 *  @author    Brandon Elias Frazier
 *  @date      Aug 30, 2026
 *
 *
 *  @brief     Thread safe overwriting generic ring buffer implementation
 *
 *
 *  @copyright (c) 2026 Brandon Elias Frazier
 *
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 *
 *
 *********************************************************************************
 *
 */

package ringbuffer

import (
	"cmp"
	"errors"
	"strconv"
	"sync"
)

// Constaint to require an interface to obtain our comparison value as well as require a type that
// can be compared in the first place
type Comparable[C cmp.Ordered] interface {
	GetCmpValue() C
}

type RingBuffer[T Comparable[C], C cmp.Ordered] struct {
	buf      []T
	head     int
	tail     int
	count    int
	ringLock sync.RWMutex
}

// Create new ring buffer with capacity blocks.
// Returns an error if capacity is an invalid size
func New[T Comparable[C], C cmp.Ordered](capacity int) (*RingBuffer[T, C], error) {
	if capacity <= 0 {
		return nil, errors.New("Invalid capacity '" + strconv.Itoa(capacity) + "'")
	}

	return &RingBuffer[T, C]{
		buf: make([]T, capacity),
	}, nil
}

// Returns the number of elements currently stored.
func (r *RingBuffer[T, C]) Len() int {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()
	return r.count
}

// Returns the capacity of the buffer.
func (r *RingBuffer[T, C]) Cap() int {
	// NOTE(BEF): This will need a lock if we ever change cap after creation.
	return len(r.buf)
}

// Returns whether the buffer length is capacity.
func (r *RingBuffer[T, C]) IsFull() bool {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()
	return r.count == len(r.buf)
}

// Returns whether or not the buffer is empty. Revolutionary I know
func (r *RingBuffer[T, C]) IsEmpty() bool {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()
	return r.count == 0
}

// Adds element to ring buffer.
// Should the buffer be full the oldest element is written over. In this case the value will be
// returned through evicted and didEvict flag will be set true.
func (r *RingBuffer[T, C]) Push(v T) (evicted T, didEvict bool) {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	if r.count == len(r.buf) {
		evicted = r.buf[r.tail]
		didEvict = true
		r.tail = r.next(r.tail)
		r.count--
	}
	r.buf[r.head] = v
	r.head = r.next(r.head)
	r.count++
	return evicted, didEvict
}

// Should the buffer not be empty Returns the element at the tail and true, or the zero value and
// false if the buffer is empty.
// This is all done without moving the tail
func (r *RingBuffer[T, C]) Oldest() (T, bool) {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()

	if r.count == 0 {
		var nullValue T
		return nullValue, false
	}

	return r.buf[r.tail], true
}

// Should the buffer not be empty returns the element at the head and true, or the zero value and
// false if the buffer is empty
// This is all done without moving the head
func (r *RingBuffer[T, C]) Newest() (T, bool) {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()

	if r.count == 0 {
		var nullValue T
		return nullValue, false
	}

	return r.buf[r.prev(r.head)], true
}

// Destroys elements from the tail whose comparison value is less than the cutoff.
// Element comparison stops when an element passes cutoff check
func (r *RingBuffer[T, C]) TrimLessThan(cutoff C) {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	for (r.count > 0) && (cmp.Less(r.buf[r.tail].GetCmpValue(), cutoff)) {
		r.tail = r.next(r.tail)
		r.count--
	}
}

// Gets elements from the tail whose comparison value is less than the cutoff.
// Element comparison stops when an element passes cutoff check
func (r *RingBuffer[T, C]) GetLessThan(cutoff C) ([]T) {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	out := make([]T, 0)
	for (r.count > 0) && (cmp.Less(r.buf[r.tail].GetCmpValue(), cutoff)) {
		out = append(out, r.buf[r.tail])
		r.tail = r.next(r.tail)
		r.count--
	}
	
	if len(out) == 0 {
		return nil
	}

	return out
}

// Destroys elements from the tail whose comparison value is more than the cutoff.
// Element comparison stops when an element passes cutoff check
func (r *RingBuffer[T, C]) TrimMoreThan(cutoff C) {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	for (r.count > 0) && (cmp.Less(cutoff, r.buf[r.tail].GetCmpValue())) {
		r.tail = r.next(r.tail)
		r.count--
	}
}

// Resets head tail and count back to 0.
func (r *RingBuffer[T, C]) Reset() {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	r.head = 0
	r.tail = 0
	r.count = 0
}

// TODO(BEF): This really shouldn't be the way to do this (expensive obv), but I'm still thinking
//			  through how to prevent overwriting while the consumer is eating the this snapshot
//			  window
// Returns a newly allocated slice containing a copy of all elements ordered from tail to head.
func (r *RingBuffer[T, C]) Snapshot() []T {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()

	out := make([]T, r.count)
	for i := 0; i < r.count; i++ {
		out[i] = r.buf[(r.tail+i)%len(r.buf)]
	}
	return out
}

// Calls fn for every element from tail to head Stops at the point at which fn returns false.
//
// NOTE(BEF): There is a possibility that calling the fingbuffer functions within this function
//			  will cause a deadlock. Instead do what you need within the function and do your
//			  ringbuffer operations afterwards
func (r *RingBuffer[T, C]) ForEach(fn func(T) bool) {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()

	for i := 0; i < r.count; i++ {
		if !fn(r.buf[(r.tail+i)%len(r.buf)]) {
			return
		}
	}
}

// Internal convenience functions
func (r *RingBuffer[T, C]) next(i int) int {
	i++
	if i == len(r.buf) {
		return 0
	}
	return i
}

func (r *RingBuffer[T, C]) prev(i int) int {
	if i == 0 {
		return len(r.buf) - 1
	}
	return i - 1
}
