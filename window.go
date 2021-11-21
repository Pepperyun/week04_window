package window

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	success uint32 = iota
	err
	timeoutSuccess
	timeoutError
)

type windowMetrics interface {
	record(outcome uint32)
}

type counts struct {
	totalCount   int64
	errorCount   int64
	timeoutCount int64
}

func (p *counts) record(outcome uint32) {
	p.totalCount++
	switch outcome {
	case err:
		p.errorCount++
	case timeoutError:
		p.errorCount++
		p.timeoutCount++
	case timeoutSuccess:
		p.timeoutCount++
	}
}

type totalCounts struct {
	counts
}

func (t *totalCounts) removeCounts(b *counts) {
	t.errorCount -= b.errorCount
	t.timeoutCount -= b.timeoutCount
	t.totalCount -= b.totalCount
}

//================================= timeWindow ====================================]

type timeWindow struct {
	capacity uint32
	buckets  buckets

	endIndex int
	lck      sync.Mutex

	totals totalCounts
}

func (t *timeWindow) record(outcome uint32) {
	t.lck.Lock()
	defer t.lck.Unlock()

	now := time.Now()

	t.moveWindow(now.Unix()).counts.record(outcome)
	t.totals.counts.record(outcome)
}

func (t *timeWindow) moveWindow(now int64) *bucket {
	currTime := now

	lastBucket := t.buckets[t.endIndex]
	timeDiff := currTime - atomic.LoadInt64(&lastBucket.startTime)

	if timeDiff == 0 {
		return lastBucket
	}

	secondsToMoveWindow := int64min(timeDiff, int64(t.capacity))

	var currentBucket *bucket

	for {
		secondsToMoveWindow--
		t.endIndex = (t.endIndex + 1) % int(t.capacity)
		currentBucket = t.buckets[t.endIndex]

		t.totals.removeCounts(&currentBucket.counts)
		currentBucket.reset(currTime - secondsToMoveWindow)

		if secondsToMoveWindow <= 0 {
			if secondsToMoveWindow < 0 {
				fmt.Printf("secondsToMoveWindow_below_0")
			}
			break
		}
	}

	return currentBucket
}

type buckets []*bucket

type bucket struct {
	counts
	startTime int64
}

func (b *bucket) reset(currEpochSecond int64) {
	b.startTime = currEpochSecond
	b.totalCount = 0
	b.errorCount = 0
	b.timeoutCount = 0
}

func int64min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
