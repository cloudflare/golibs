// package tokenbucket implements a simple token bucket filter.
package tokenbucket

import (
	"math/rand"
	"time"
)

type item struct {
	credit int64
	prev   int64
}

// Filter implements a token bucket filter.
type Filter struct {
	creditMax int64
	touchCost int64

	key0 uint64
	key1 uint64

	items []item
}

// New creates a new token bucket filter with num buckets, accruing tokens at rate per second. The depth of the
// bucket is rate * 1s * burst.
func New(num int, rate int64, burst float64) *Filter {
	b := new(Filter)
	if burst <= 0 {
		panic("burst factor must be greater than 0")
	}
	b.touchCost = int64(1*time.Second) / rate
	b.creditMax = int64(burst * float64(1*time.Second))
	b.items = make([]item, num)

	// Not the full range of a uint64, but we can
	// live with 2 bits of entropy missing
	b.key0 = uint64(rand.Int63())
	b.key1 = uint64(rand.Int63())

	t := time.Now().UnixNano()
	for i := range b.items {
		b.items[i].credit = b.creditMax
		b.items[i].prev = t
	}

	return b
}

func (b *Filter) touch(it *item) bool {
	now := time.Now().UnixNano()
	delta := now - it.prev
	it.credit += delta
	it.prev = now

	if it.credit > b.creditMax {
		it.credit = b.creditMax
	}

	if it.credit > b.touchCost {
		it.credit -= b.touchCost
		return true
	}
	return false
}

// Touch finds the token bucket for d, takes a token out of it and reports if
// there are still tokens left in the bucket.
func (b *Filter) Touch(d []byte) bool {
	n := len(b.items)
	h := hash(b.key0, b.key1, d)
	i := int(h) % n
	return b.touch(&b.items[i])
}
