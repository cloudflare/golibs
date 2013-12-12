// Copyright (c) 2013 CloudFlare, Inc.

package circularbuffer

import (
	"testing"
)

func (b *CircularBuffer) verifyIsEmpty() bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	e := len(b.avail) == 0
	if e {
		if b.pos != b.start {
			panic("desychronized state")
		}
	}
	return e
}

func TestSyncGet(t *testing.T) {
	c := NewCircularBuffer(10)

	for i := 0; i < 4; i++ {
		c.NBPush(i)
	}

	for i := 0; i < 4; i++ {
		v := c.Get().(int)
		if i != v {
			t.Error(v)
		}
	}

	if c.verifyIsEmpty() != true {
		t.Error("not empty")
	}
}

func TestSyncOverflow(t *testing.T) {
	c := NewCircularBuffer(10) // up to 9 items in the buffer

	for i := 0; i < 9; i++ {
		v := c.NBPush(i)
		if v != nil {
			t.Error(v)
		}
	}
	v := c.NBPush(9)
	if v != 0 {
		t.Error(v)
	}

	for i := 1; i < 10; i++ {
		v := c.Get().(int)
		if i != v {
			t.Error(v)
		}
	}

	if c.verifyIsEmpty() != true {
		t.Error("not empty")
	}
}

func TestAsyncGet(t *testing.T) {
	c := NewCircularBuffer(10)

	go func() {
		for i := 0; i < 4; i++ {
			v := c.Get().(int)
			if i != v {
				t.Error(i)
			}
		}

		if c.verifyIsEmpty() != true {
			t.Error("not empty")
		}
	}()

	c.NBPush(0)
	c.NBPush(1)
	c.NBPush(2)
	c.NBPush(3)
}

func TestSyncPop(t *testing.T) {
	c := NewCircularBuffer(10)

	c.NBPush(3)
	c.NBPush(2)
	c.NBPush(1)
	c.NBPush(0)

	for i := 0; i < 4; i++ {
		v := c.Pop().(int)
		if i != v {
			t.Error(v)
		}
	}

	if c.verifyIsEmpty() != true {
		t.Error("not empty")
	}
}

func TestASyncPop(t *testing.T) {
	c := NewCircularBuffer(10)

	go func() {
		for i := 0; i < 4; i++ {
			v := c.Pop().(int)
			if i != v {
				t.Error(v)
			}
		}

		if c.verifyIsEmpty() != true {
			t.Error("not empty")
		}
	}()

	c.NBPush(3)
	c.NBPush(2)
	c.NBPush(1)
	c.NBPush(0)
}

func TestSyncOverflowEvictCallback(t *testing.T) {
	c := NewCircularBuffer(10) // up to 9 items in the buffer

	evicted := 0
	c.Evict = func(v interface{}) {
		if v.(int) != evicted {
			t.Error(v)
		}
		evicted += 1
	}

	for i := 0; i < 18; i++ {
		v := c.NBPush(i)
		if v != nil {
			t.Error(v)
		}
	}

	for i := 9; i < 18; i++ {
		v := c.Get().(int)
		if i != v {
			t.Error(v)
		}
	}

	if evicted != 9 {
		t.Error(evicted)
	}

	if c.verifyIsEmpty() != true {
		t.Error("not empty")
	}
}
