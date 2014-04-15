// Copyright (c) 2014 CloudFlare, Inc.

package spacesaving

import (
	"math"
	"time"
)

type srateBucket struct {
	key       string
	count     uint64
	countTs   int64
	countRate float64
	error     uint64
	errorTs   int64
	errorRate float64
}

type SimpleRate struct {
	olist        []srateBucket
	hash         map[string]uint32
	weightHelper float64
	halfLife     time.Duration
}

func (ss *SimpleRate) Init(size int, halfLife time.Duration) *SimpleRate {
	*ss = SimpleRate{
		olist:        make([]srateBucket, size),
		hash:         make(map[string]uint32, size),
		weightHelper: -math.Ln2 / float64(halfLife.Nanoseconds()),
		halfLife:     halfLife,
	}
	return ss
}

func (ss *SimpleRate) count(rate float64, lastTs, now int64) float64 {
	deltaNs := float64(now - lastTs)
	weight := math.Exp(deltaNs * ss.weightHelper)

	if deltaNs > 0 && lastTs != 0 {
		return rate*weight + (1000000000./deltaNs)*(1-weight)
	}
	return rate * weight
}

func (ss *SimpleRate) recount(rate float64, lastTs, now int64) float64 {
	return rate * math.Exp(float64(now-lastTs)*ss.weightHelper)
}

func (ss *SimpleRate) Touch(key string, nowTs time.Time) {
	var (
		bucketno uint32
		found    bool
		bucket   *srateBucket
		now      = nowTs.UnixNano()
	)

	if bucketno, found = ss.hash[key]; found {
		bucket = &ss.olist[bucketno]
	} else {
		bucketno = 0
		bucket = &ss.olist[bucketno]
		delete(ss.hash, bucket.key)
		ss.hash[key] = bucketno
		bucket.error, bucket.errorTs, bucket.errorRate =
			bucket.count, bucket.countTs, bucket.countRate
		bucket.key = key
	}

	bucket.count += 1
	bucket.countRate = ss.count(bucket.countRate, bucket.countTs, now)
	bucket.countTs = now

	for {
		if bucketno == uint32(len(ss.olist))-1 {
			break
		}

		b1 := &ss.olist[bucketno]
		b2 := &ss.olist[bucketno+1]
		if b1.countTs < b2.countTs {
			break
		}

		ss.hash[b1.key] = bucketno + 1
		ss.hash[b2.key] = bucketno
		*b1, *b2 = *b2, *b1
		bucketno += 1
	}
}

type srateElement struct {
	Key     string
	LoCount uint64
	HiCount uint64
	LoRate  float64
	HiRate  float64
}

func (ss *SimpleRate) GetAll(nowTs time.Time) []srateElement {
	now := nowTs.UnixNano()

	elements := make([]srateElement, 0, len(ss.hash))
	for i := len(ss.olist) - 1; i >= 0; i -= 1 {
		b := &ss.olist[i]
		if b.key == "" {
			continue
		}
		rate := ss.recount(b.countRate, b.countTs, now)
		errRate := ss.recount(b.errorRate, b.errorTs, now)
		elements = append(elements, srateElement{
			Key:     b.key,
			LoCount: b.count - b.error,
			HiCount: b.count,
			LoRate:  rate - errRate,
			HiRate:  rate,
		})
	}
	return elements
}
