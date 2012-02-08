package kc

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lkyotocabinet
#include <kclangc.h>
*/
import "C"

import (
	"unsafe"
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
	finish chan int
}

func NewApplyResult() *ApplyResult {
	return &ApplyResult{finish: make(chan int)}
}

func (r *ApplyResult) Wait() {
	<-r.finish
}

// Applies a function to all records in the database
//
// The function is called with the key and the value as parameters.
// All extra arguments passed to Apply are used in the call to f
func (d *DB) Apply(f ApplyFunc, args ...interface{}) {
	var keyLen, valueLen C.size_t
	var valueBuffer *C.char
	defer C.free(unsafe.Pointer(valueBuffer))

	cur := C.kcdbcursor(d.db)
	defer C.kccurdel(cur)

	C.kccurjump(cur)

	var next = func() *C.char {
		return C.kccurget(cur, &keyLen, &valueBuffer, &valueLen, 1)
	}

	for keyBuffer := next(); keyBuffer != nil; keyBuffer = next() {
		key := C.GoString(keyBuffer)
		C.free(unsafe.Pointer(keyBuffer))

		value := C.GoString(valueBuffer)
		f(key, value, args...)
	}
}

// Similar to Apply, but asynchronous. Returns a Waiter object, so
// you can wait for the applying to finish
func (d *DB) AsyncApply(f ApplyFunc, args ...interface{}) Waiter {
	r := NewApplyResult()
	go func() {
		d.Apply(f, args...)
		r.finish <- 1
	}()

	return r
}
