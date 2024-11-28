// package tokenbucket implements a simple token bucket filter.
package tokenbucket

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type item struct {
	credit uint64
	prev   uint64
}

// Filter implements a token bucket filter.
type Filter struct {
	creditMax uint64
	touchCost uint64

	key0 uint64
	key1 uint64

	items []item
}

// New creates a new token bucket filter with num buckets, accruing tokens at rate per second. The depth specifies
// the depth of the bucket.
func New(num int, rate float64, depth uint64) *Filter {
	b := new(Filter)
	if depth <= 0 {
		panic("depth of bucket must be greater than 0")
	}
	b.touchCost = uint64((float64(1*time.Second) / rate))
	b.creditMax = depth * b.touchCost
	b.items = make([]item, num)

	// Not the full range of a uint64, but we can
	// live with 2 bits of entropy missing
	b.key0 = uint64(rand.Int63())
	b.key1 = uint64(rand.Int63())

	return b
}

func (b *Filter) touch(it *item) bool {
	now := uint64(time.Now().UnixNano())
	oldPrev := atomic.LoadUint64(&it.prev)
	oldCredit := atomic.LoadUint64(&it.credit)

	delta := now - oldPrev
	defer atomic.StoreUint64(&it.prev, now)

	newCredit := oldCredit + delta
	allow := false

	if newCredit > b.creditMax {
		newCredit = b.creditMax
	}
	if newCredit > b.touchCost {
		newCredit -= b.touchCost
		allow = true
	}
	atomic.StoreUint64(&it.credit, newCredit)

	return allow
}

// Touch finds the token bucket for d, takes a token out of it and reports if
// there are still tokens left in the bucket.
func (b *Filter) Touch(d []byte) bool {
	n := len(b.items)
	h := hash(b.key0, b.key1, d)
	i := h % uint64(n)
	return b.touch(&b.items[i])
}
