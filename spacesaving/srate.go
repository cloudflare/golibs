// Copyright (c) 2014 CloudFlare, Inc.

package spacesaving

import (
	"container/heap"
	"math"
	"time"
	"sort"
)

type srateBucket struct {
	key       string
	countTs   int64
	countRate float64
	errorTs   int64
	errorRate float64
	index     int
}

type srateHeap []*srateBucket

func (sh srateHeap) Len() int { return len(sh) }

func (sh srateHeap) Less(i, j int) bool {
	return sh[i].countTs < sh[j].countTs
}

func (sh srateHeap) Swap(i, j int) {
	sh[i], sh[j] = sh[j], sh[i]
	sh[i].index = i
	sh[j].index = j
}

func (sh *srateHeap) Push(x interface{}) {
	n := len(*sh)
	bucket := x.(*srateBucket)
	bucket.index = n
	*sh = append(*sh, bucket)
}

func (sh *srateHeap) Pop() interface{} {
	old := *sh
	n := len(old)
	bucket := old[n-1]
	bucket.index = -1 // for safety
	*sh = old[0 : n-1]
	return bucket
}

type SimpleRate struct {
	heap         srateHeap
	hash         map[string]*srateBucket
	weightHelper float64
	halfLife     time.Duration
	size         int
}

func (ss *SimpleRate) Init(size int, halfLife time.Duration) *SimpleRate {
	*ss = SimpleRate{
		heap:         make([]*srateBucket, 0, size),
		hash:         make(map[string]*srateBucket, size),
		weightHelper: -math.Ln2 / float64(halfLife.Nanoseconds()),
		halfLife:     halfLife,
		size:         size,
	}
	return ss
}

func (ss *SimpleRate) count(rate float64, lastTs, now int64, userWeight float64) float64 {
	deltaNs := float64(now - lastTs)
	weight := math.Exp(deltaNs * ss.weightHelper)

	if deltaNs > 0 && lastTs != 0 {
		return rate*weight + (1000000000./deltaNs)*userWeight*(1-weight)
	}
	return rate * weight
}

func (ss *SimpleRate) recount(rate float64, lastTs, now int64) float64 {
	return rate * math.Exp(float64(now-lastTs)*ss.weightHelper)
}

func (ss *SimpleRate) findBucket(key string) *srateBucket {
	bucket, found := ss.hash[key];
	if found {
		// we already have the correct bucket
	} else if len(ss.heap) < ss.size {
		// create new bucket
		bucket = &srateBucket{}
		ss.hash[key] = bucket
		bucket.key = key
		heap.Push(&ss.heap, bucket)
	} else {
		// use minimum bucket
		bucket = ss.heap[0]
		delete(ss.hash, bucket.key)
		ss.hash[key] = bucket
		bucket.errorTs, bucket.errorRate =
			bucket.countTs, bucket.countRate
		bucket.key = key
	}
	return bucket
}

func (ss *SimpleRate) Touch(key string, nowTs time.Time) {
	var (
		now      = nowTs.UnixNano()
		bucket   = ss.findBucket(key)
	)

	bucket.countRate = ss.count(bucket.countRate, bucket.countTs, now, 1)
	bucket.countTs = now
	heap.Fix(&ss.heap, bucket.index)
}

func (ss *SimpleRate) TouchWeight(key string, nowTs time.Time, userWeight float64) {
	var (
		now      = nowTs.UnixNano()
		bucket   = ss.findBucket(key)
	)

	bucket.countRate = ss.count(bucket.countRate, bucket.countTs, now, userWeight)
	bucket.countTs = now
	heap.Fix(&ss.heap, bucket.index)
}

func (ss *SimpleRate) Set(key string, nowTs time.Time, rate float64) {
	var (
		now      = nowTs.UnixNano()
		bucket   = ss.findBucket(key)
	)

	bucket.countRate = rate
	bucket.countTs = now
	heap.Fix(&ss.heap, bucket.index)
}

type srateElement struct {
	Key     string
	LoRate  float64
	HiRate  float64
}

type sserSlice []srateElement

func (a sserSlice) Len() int           { return len(a) }
func (a sserSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sserSlice) Less(i, j int) bool { return a[i].LoRate < a[j].LoRate }

func (ss *SimpleRate) GetAll(nowTs time.Time) []srateElement {
	now := nowTs.UnixNano()

	elements := make([]srateElement, 0, len(ss.heap))
	for _, b := range ss.heap {
		rate := ss.recount(b.countRate, b.countTs, now)
		errRate := ss.recount(b.errorRate, b.errorTs, now)
		elements = append(elements, srateElement{
			Key:     b.key,
			LoRate:  rate - errRate,
			HiRate:  rate,
		})
	}
	sort.Sort(sort.Reverse(sserSlice(elements)))
	return elements
}

func (ss *SimpleRate) GetSingle(key string, nowTs time.Time) (float64, float64) {
	now := nowTs.UnixNano()
	if bucket, found := ss.hash[key]; found {
		//bucket = &ss.buckets[bucketno]
		rate := ss.recount(bucket.countRate, bucket.countTs, now)
		errRate := ss.recount(bucket.errorRate, bucket.errorTs, now)
		return rate - errRate, rate
	} else {
		bucket = ss.heap[0]
		//bucket = &ss.buckets[bucketno]
		errRate := ss.recount(bucket.countRate, bucket.countTs, now)
		return 0, errRate
	}

}

