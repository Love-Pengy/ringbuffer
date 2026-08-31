/**
 *  @file      ringbuffer_test.go
 *  @author    Brandon Elias Frazier
 *  @date      Aug 30, 2026
 *
 *
 *  @brief     Tests for ringbuffer.
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
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type frame struct {
	ts  int64
	val int
}

func (f frame) GetCmpValue() int64 { return f.ts }

func makeFrame(seq int, ts int64) frame { return frame{ts: ts, val: seq} }

type floatFrame struct {
	ts  float64
	seq int
}

func (f floatFrame) GetCmpValue() float64 { return f.ts }

type stringFrame struct {
	ts  string
	seq int
}

func TestNewParamChecking(t *testing.T) {
	cases := []int{0, -1, -100}
	for _, c := range cases {
		t.Run(fmt.Sprintf("capacity=%d", c), func(t *testing.T) {
			r, err := New[frame, int64](c)
			if err == nil {
				t.Fatalf("Expected error for capacity %d, got nil (ringBuff=%v)", c, r)
			}

			if r != nil {
				t.Fatalf("Expected nil ring buffer on error, got %v", r)
			}
		})
	}
}

func TestNewValidParam(t *testing.T) {
	ring, err := New[frame, int64](4)
	if err != nil {
		t.Fatal("Unexpected error for valid capacity 4: " + err.Error())
	}

	if ring.Cap() != 4 {
		t.Fatalf("cap = %d expected 4", ring.Cap())
	}

	if ring.Len() != 0 {
		t.Fatalf("len = %d expected 0", ring.Len())
	}

	if !ring.IsEmpty() {
		t.Fatal("Expected new buffer to be empty")
	}

	if ring.IsFull() {
		t.Fatal("Expected new buffer to not be full")
	}
}

func TestPushWithinBounds(t *testing.T) {
	ring := must(New[frame, int64](3))
	for i := range 3 {
		evicted, didEvict := ring.Push(makeFrame(i, int64(i)))
		if didEvict {
			t.Fatalf("Push #%d: unexpected eviction of %v", i, evicted)
		}
	}

	if length := ring.Len(); length != 3 {
		t.Fatalf("Len = %d expected 3", length)
	}

	if !ring.IsFull() {
		t.Fatal("Expected buffer to be full after 3 additions to buffer of size 3")
	}
}

func TestLandlord(t *testing.T) {
	ring := must(New[frame, int64](3))
	ring.Push(makeFrame(0, 0))
	ring.Push(makeFrame(1, 1))
	ring.Push(makeFrame(2, 2))

	evicted, didEvict := ring.Push(makeFrame(3, 3))
	if !didEvict {
		t.Fatal("Expected eviction on push into full buffer")
	}

	if evicted.val != 0 {
		t.Fatalf("Evicted seq = %d. Expected value of 0", evicted.val)
	}

	if length := ring.Len(); length != 3 {
		t.Fatalf("length = %d expected 3", length)
	}

	oldest, ok := ring.Oldest()
	if (!ok) || (oldest.val != 1) {
		t.Fatalf("oldest = %v, ok = %v; expectedc value of 1", oldest, ok)
	}

	newest, ok := ring.Newest()
	if (!ok) || (newest.val != 3) {
		t.Fatalf("newest = %v, ok = %v expected  value of 3", newest, ok)
	}
}

func TestPushWrap(t *testing.T) {
	ring := must(New[frame, int64](4))
	const total = 25
	for i := range total {
		ring.Push(makeFrame(i, int64(i)))
	}

	want := []int{21, 22, 23, 24} // last 4 pushed, oldest-first
	got := frameArrToIntArr(ring.Snapshot())
	if !equalIntArr(got, want) {
		t.Fatalf("Snapshot values = %v, expected %v", got, want)
	}
}

func TestOldestNewestOnEmptyBuffer(t *testing.T) {
	ring := must(New[frame, int64](2))
	if _, ok := ring.Oldest(); ok {
		t.Fatal("oldest on empty buffer should return ok=false")
	}

	if _, ok := ring.Newest(); ok {
		t.Fatal("newest  on empty buffer should return ok=false")
	}
}

func TestNiceLandlord(t *testing.T) {
	ring := must(New[frame, int64](2))
	evicted, didEvict := ring.Push(makeFrame(0, 0))
	if didEvict {
		t.Fatal("Did not expect eviction on first push")
	}

	var zero frame
	if evicted != zero {
		t.Fatalf("Evicted = %v expected zero value", evicted)
	}
}

func TestTrimLessThanBase(t *testing.T) {
	ring := must(New[frame, int64](5))
	for i := range 5 {
		ring.Push(makeFrame(i, int64(i*10)))
	}

	ring.TrimLessThan(25)

	want := []int{3, 4}
	got := frameArrToIntArr(ring.Snapshot())
	if !equalIntArr(got, want) {
		t.Fatalf("trim values = %v expected %v", got, want)
	}
}

func TestTrimLessThanStopPoint(t *testing.T) {
	ring := must(New[frame, int64](4))
	ring.Push(makeFrame(0, 10))
	ring.Push(makeFrame(1, 20))
	ring.Push(makeFrame(2, 5))
	ring.Push(makeFrame(3, 30))

	ring.TrimLessThan(15)

	want := []int{1, 2, 3}
	got := frameArrToIntArr(ring.Snapshot())
	if !equalIntArr(got, want) {
		t.Fatalf("Values= %v expected %v", got, want)
	}
}

func TestTrimLessThanNothingCase(t *testing.T) {
	ring := must(New[frame, int64](3))
	ring.Push(makeFrame(0, 100))
	ring.Push(makeFrame(1, 200))
	ring.TrimLessThan(0)

	want := []int{0, 1}
	got := frameArrToIntArr(ring.Snapshot())
	if !equalIntArr(got, want) {
		t.Fatalf("Values = %v expected %v", got, want)
	}
}

func TestTrimLessThanEmptyBuffer(t *testing.T) {
	ring := must(New[frame, int64](3))
	ring.Push(makeFrame(0, 1))
	ring.Push(makeFrame(1, 2))

	ring.TrimLessThan(1000)

	if !ring.IsEmpty() {
		t.Fatalf("Expected buffer to be empty length= %d", ring.Len())
	}

	if _, ok := ring.Oldest(); ok {
		t.Fatal("Oldest expected to fail on empty buffer")
	}
}

func TestTrimLessThanOnEmptyBuffer(t *testing.T) {
	ring := must(New[frame, int64](3))

	ring.TrimLessThan(100)

	if ring.Len() != 0 {
		t.Fatalf("length = %d expected 0", ring.Len())
	}
}

func TestTrimMoreThanBase(t *testing.T) {
	ring := must(New[frame, int64](5))
	ring.Push(makeFrame(0, 50))
	ring.Push(makeFrame(1, 40))
	ring.Push(makeFrame(2, 10))
	ring.Push(makeFrame(3, 5))

	ring.TrimMoreThan(30)

	want := []int{2, 3}
	got := frameArrToIntArr(ring.Snapshot())
	if !equalIntArr(got, want) {
		t.Fatalf("Values = %v expected %v", got, want)
	}
}

func TestTrimMoreThanDoNothingCase(t *testing.T) {
	ring := must(New[frame, int64](3))
	ring.Push(makeFrame(0, 1))
	ring.Push(makeFrame(1, 100))

	ring.TrimMoreThan(50)

	want := []int{0, 1}
	got := frameArrToIntArr(ring.Snapshot())
	if !equalIntArr(got, want) {
		t.Fatalf("Values = %v expected %v", got, want)
	}
}

func TestTrimMoreThanOnEmptyBuffer(t *testing.T) {
	ring := must(New[frame, int64](3))

	ring.TrimMoreThan(0)

	if length := ring.Len(); length != 0 {
		t.Fatalf("Length = %d expected 0", ring.Len())
	}
}

func TestResetReuse(t *testing.T) {
	ring := must(New[frame, int64](3))
	ring.Push(makeFrame(0, 0))
	ring.Push(makeFrame(1, 1))

	ring.Reset()

	if !ring.IsEmpty() {
		t.Fatal("Expected buffer to be empty after Reset")
	}
	if length := ring.Len(); length != 0 {
		t.Fatalf("Lenght= %d Expected 0", ring.Len())
	}

	ring.Push(makeFrame(2, 2))
	newest, ok := ring.Newest()
	if (!ok) || (newest.val != 2) {
		t.Fatalf("newest after reset+push = %v, ok = %v expected value 2", newest, ok)
	}
}

func TestSnapshotIsIndependentCopy(t *testing.T) {
	ring := must(New[frame, int64](3))
	ring.Push(makeFrame(0, 0))
	ring.Push(makeFrame(1, 1))

	snap := ring.Snapshot()
	ring.Push(makeFrame(2, 2))
	ring.Push(makeFrame(3, 3))

	want := []int{0, 1}
	got := frameArrToIntArr(snap)
	if !equalIntArr(got, want) {
		t.Fatalf("Unexpected snapshot values = %v expected %v", got, want)
	}
}

func TestSnapshotEmptyBuffer(t *testing.T) {
	ring := must(New[frame, int64](3))
	snap := ring.Snapshot()

	if length := len(snap); length != 0 {
		t.Fatalf("length of snapshot = %d expected 0", length)
	}
}

func TestForEachOrderAndEarlyStop(t *testing.T) {
	ring := must(New[frame, int64](5))
	for i := range 5 {
		ring.Push(makeFrame(i, int64(i)))
	}

	var touched []int
	ring.ForEach(func(f frame) bool {
		touched = append(touched, f.val)
		return f.val < 2
	})

	want := []int{0, 1, 2}
	if !equalIntArr(touched, want) {
		t.Fatalf("touched = %v expected %v", touched, want)
	}
}

func TestForEachFull(t *testing.T) {
	ring := must(New[frame, int64](4))
	for i := range 4 {
		ring.Push(makeFrame(i, int64(i)))
	}

	var touched []int
	ring.ForEach(func(f frame) bool {
		touched = append(touched, f.val)
		return true
	})

	want := []int{0, 1, 2, 3}
	if !equalIntArr(touched, want) {
		t.Fatalf("touched = %v expected %v", touched, want)
	}
}

func (f stringFrame) GetCmpValue() string { return f.ts }

func TestGenericComparisonTypeFloat64(t *testing.T) {
	ring := must(New[floatFrame, float64](3))
	ring.Push(floatFrame{ts: 0.1, seq: 0})
	ring.Push(floatFrame{ts: 0.2, seq: 1})
	ring.Push(floatFrame{ts: 0.3, seq: 2})

	ring.TrimLessThan(0.25)

	got := ring.Snapshot()
	if (len(got) != 1) || (got[0].seq != 2) {
		t.Fatalf("Snapshot = %v expected single element seq=2", got)
	}
}

func TestGenericComparisonTypeString(t *testing.T) {
	ring := must(New[stringFrame, string](3))
	ring.Push(stringFrame{ts: "2024-01-01", seq: 0})
	ring.Push(stringFrame{ts: "2024-06-01", seq: 1})
	ring.Push(stringFrame{ts: "2025-01-01", seq: 2})

	ring.TrimLessThan("2024-12-31")

	got := ring.Snapshot()
	if (len(got) != 1) || (got[0].seq != 2) {
		t.Fatalf("Snapshot = %v expected single element seq=2", got)
	}
}

func TestMultiThreadNoRace(t *testing.T) {
	const (
		capacity   = 16
		numPushers = 8
		pushesEach = 2000
		numReaders = 4
	)
	ring := must(New[frame, int64](capacity))

	var valCounter atomic.Int64
	var pushWG, readerWG sync.WaitGroup

	for range numPushers {
		pushWG.Go(func() {
			for range pushesEach {
				s := int(valCounter.Add(1))
				ring.Push(makeFrame(s, time.Now().UnixNano()))
			}
		})
	}

	stop := make(chan struct{})

	for range numReaders {
		readerWG.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = ring.Len()
				_ = ring.IsFull()
				_ = ring.IsEmpty()
				_ = ring.Snapshot()
				ring.ForEach(func(frame) bool { return true })
			}
		})
	}

	pushWG.Go(
		func() {
			for range 200 {
				ring.TrimLessThan(time.Now().Add(-time.Hour).UnixNano())
				time.Sleep(time.Microsecond)
			}
		})

	pushDone := make(chan struct{})
	go func() {
		pushWG.Wait()
		close(pushDone)
	}()

	waitWithTimeout(t, pushDone, 30*time.Second)
	close(stop)

	readersDone := make(chan struct{})
	go func() {
		readerWG.Wait()
		close(readersDone)
	}()

	waitWithTimeout(t, readersDone, 5*time.Second)

	if ring.Len() > ring.Cap() {
		t.Fatalf("Length = %d exceeds capacity = %d after concurrent use", ring.Len(), ring.Cap())
	}

	if ring.Len() < 0 {
		t.Fatalf("Length went negative: %d", ring.Len())
	}

	snap := ring.Snapshot()
	if len(snap) != ring.Len() {
		t.Fatalf("Snapshot length %d != length of buffer %d", len(snap), ring.Len())
	}
}

func TestConcurrentResetDuringPush(t *testing.T) {
	const capacity = 8
	ring := must(New[frame, int64](capacity))

	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Go(
		func() {
			i := 0
			for {
				select {
				case <-done:
					return
				default:
					ring.Push(makeFrame(i, int64(i)))
					i++
				}
			}
		})

	wg.Go(func() {
		for range 500 {
			ring.Reset()
		}
	})

	time.Sleep(20 * time.Millisecond)
	close(done)
	finished := make(chan struct{})

	go func() {
		wg.Wait()
		close(finished)
	}()

	waitWithTimeout(t, finished, 5*time.Second)

	if (ring.Len() < 0) || (ring.Len() > ring.Cap()) {
		t.Fatalf("Incorrect lenght/cap in concurrent reset/push: length = %d capacity = %d",
			ring.Len(),
			ring.Cap())
	}
}

func TestRandomOperations(t *testing.T) {
	const capacity = 6
	rng := rand.New(rand.NewSource(42))
	ring := must(New[frame, int64](capacity))

	var model []frame
	valueCount := 0
	ts := int64(0)

	for op := range 5000 {
		switch rng.Intn(3) {
		case 0: // push
			ts++
			frame := makeFrame(valueCount, ts)
			valueCount++
			ring.Push(frame)
			model = append(model, frame)
			if len(model) > capacity {
				model = model[1:]
			}
		case 1: // trim older
			cutoff := ts - int64(rng.Intn(4))
			ring.TrimLessThan(cutoff)
			i := 0
			for i < len(model) && model[i].ts < cutoff {
				i++
			}
			model = model[i:]
		case 2: // reset occasionally
			if rng.Intn(20) == 0 {
				ring.Reset()
				model = nil
			}
		}

		if ring.Len() != len(model) {
			t.Fatalf("Model len doesn't match ring length operation %d: length = %d model len = %d", op, ring.Len(), len(model))
		}

		got := frameArrToIntArr(ring.Snapshot())
		want := frameArrToIntArr(model)
		if !equalIntArr(got, want) {
			t.Fatalf("operation %d: values= %v expected %v", op, got, want)
		}
	}
}

// Convenience functions
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func frameArrToIntArr(fs []frame) []int {
	out := make([]int, len(fs))
	for i, f := range fs {
		out[i] = f.val
	}
	return out
}

func equalIntArr(a, b []int) bool {
	if 0 != slices.Compare(a, b) {
		return false
	}
	return true
}

func waitWithTimeout(t *testing.T, done <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for goroutines; possible deadlock")
	}
}
