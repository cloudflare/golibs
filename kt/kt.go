// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package kt

/*
#cgo pkg-config: kyototycoon
#include <ktlangc.h>
#include "kt.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const (
	_ = iota
	DEFAULT_TIMEOUT = -1.
)

// KTError is used for errors using the gokabinet library.  It implements the
// builtin error interface.
type KTError string

func (err KTError) Error() string {
	return string(err)
}

// DB is the basic type for the gokabinet library. Holds an unexported instance
// of the database, for interactions.
type RemoteDB struct {
	db   *C.KTRDB
}

// Open opens a connection to a remote database.
//
func Open(host string, port int, timeout float32) (*RemoteDB, error) {
	d := &RemoteDB{db: C.ktdbnew()}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	cPort := C.int32_t(port)
	cTimeout := C.double(timeout)

	if C.ktdbopen(d.db, cHost, cPort, cTimeout) == 0 {
		errMsg := d.LastError()
		return nil, KTError(fmt.Sprintf("Error opening %s:%d -- %s", host, port, errMsg))
	}
	return d, nil
}

// LastError returns a KTError instance representing the last occurred error in
// the database.
func (d *RemoteDB) LastError() error {
	errMsg := C.GoString(C.ktecodename(C.ktdbecode(d.db)))
	return KTError(errMsg)
}

// Close the connection to the db
func (d *RemoteDB) Close() {
	if (d != nil) {
		C.ktdbclose(d.db)
		C.ktdbdel(d.db)
	}
}

func (d *RemoteDB) Set(key, value string) error {

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	lKey := C.size_t(len(key))
	lValue := C.size_t(len(value))
	if C.ktdbset(d.db, cKey, lKey, cValue, lValue) == 0 {
		errMsg := d.LastError()
		return KTError(fmt.Sprintf("Failed to add a record with the value %s and the key %s: %s", value, key, errMsg))
	}
	return nil
}

func (d *RemoteDB) Get(key string) (string, error) {
	var resultLen C.size_t
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	cValue := C.ktdbget(d.db, cKey, lKey, &resultLen)
	defer C.free(unsafe.Pointer(cValue))
	if cValue == nil {
		errMsg := d.LastError()
		return "", KTError(fmt.Sprintf("Failed to get the record with the key %s: %s", key, errMsg))
	}
	return C.GoStringN(cValue, C.int(resultLen)), nil
}

// Returns the number of elements
func (d *RemoteDB) Count() (int, error) {
	var err error
	v := int(C.ktdbcount(d.db))
	if v == -1 {
		err = d.LastError()
	}
	return v, err
}