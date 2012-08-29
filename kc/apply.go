// Copyright 2012 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kc

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lkyotocabinet
#include <kclangc.h>
#include "kc.h"
*/
import "C"

import (
	"sync"
)

// ApplyFunc, function used as parameter to the Apply and AsyncApply
// methods. The function receives three parameters:
//
//		key: the record key
//		value: the record value
//		args: extra arguments passed to Apply method
type ApplyFunc func(key string, value interface{}, args ...interface{})

// Waiter interface, provides the Wait method
//
// This is the interface of the value returned by the AsyncApply method
type Waiter interface {
	Wait()
}

type ApplyResult struct {
	wg sync.WaitGroup
}

func NewApplyResult() *ApplyResult {
	return &ApplyResult{}
}

func (r *ApplyResult) Wait() {
	r.wg.Wait()
}

// Applies a function to all records in the database
//
// The function is called with the key and the value as parameters.
// All extra arguments passed to Apply are used in the call to f
func (d *DB) Apply(f ApplyFunc, args ...interface{}) {
	cur := C.kcdbcursor(d.db)
	defer C.kccurdel(cur)
	C.kccurjump(cur)
	for key, value := next(cur); value != ""; key, value = next(cur) {
		f(key, value, args...)
	}
}

// Similar to Apply, but asynchronous. Returns a Waiter object, so
// you can wait for the applying to finish
func (d *DB) AsyncApply(f ApplyFunc, args ...interface{}) Waiter {
	r := NewApplyResult()
	cur := C.kcdbcursor(d.db)
	defer C.kccurdel(cur)
	C.kccurjump(cur)
	for key, value := next(cur); value != ""; key, value = next(cur) {
		r.wg.Add(1)
		k, v := key, value
		go func() {
			f(k, v, args...)
			r.wg.Done()
		}()
	}
	return r
}

func next(cur *C.KCCUR) (key, value string) {
	pair := C.gokccurget(cur)
	key = C.GoString(pair.key)
	if pair.value != nil {
		value = C.GoString(pair.value)
	}
	C.free_pair(pair)
	return
}
