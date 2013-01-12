// Copyright 2013 gokabinet authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kc

/*
#cgo pkg-config: kyotocabinet
#include <kclangc.h>
*/
import "C"

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"unsafe"
)

const (
	_ = iota
	READ
	WRITE
)

// KCError is used for errors using the gokabinet library.  It implements the
// builtin error interface.
type KCError string

func (err KCError) Error() string {
	return string(err)
}

// DB is the basic type for the gokabinet library. Holds an unexported instance
// of the database, for interactions.
type DB struct {
	db   *C.KCDB
	mode int

	// Path to the database file.
	Path string
}

// Open opens a database.
//
// There are constants for the modes: READ and WRITE.
//
// READ indicates read-only access to the database, and WRITE indicates read
// and write access to the database (there is no write-only mode).
func Open(dbfilepath string, mode int) (*DB, error) {
	d := &DB{db: C.kcdbnew(), mode: mode, Path: dbfilepath}
	dbname := C.CString(dbfilepath)
	defer C.free(unsafe.Pointer(dbname))
	cMode := C.uint32_t(C.KCOREADER)
	if mode > READ {
		cMode = C.KCOWRITER | C.KCOCREATE
	}
	if C.kcdbopen(d.db, dbname, cMode) == 0 {
		errMsg := d.LastError()
		return nil, KCError(fmt.Sprintf("Error opening %s: %s", dbfilepath, errMsg))
	}
	return d, nil
}

// LastError returns a KCError instance representing the last occurred error in
// the database.
func (d *DB) LastError() error {
	errMsg := C.GoString(C.kcecodename(C.kcdbecode(d.db)))
	return KCError(errMsg)
}

// Set adds a record to the database. Currently, it's able to store only string
// values.
//
// Returns a KCError instance in case of errors, otherwise, returns nil.
func (d *DB) Set(key, value string) error {
	if d.mode < WRITE {
		return KCError("The database was opened in read-only mode, you can't add records to it")
	}
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	lKey := C.size_t(len(key))
	lValue := C.size_t(len(value))
	if C.kcdbset(d.db, cKey, lKey, cValue, lValue) == 0 {
		errMsg := d.LastError()
		return KCError(fmt.Sprintf("Failed to add a record with the value %s and the key %s: %s", value, key, errMsg))
	}
	return nil
}

// Append appends a string to the end of the value of a string record. It can't
// append any value to a numeric record.
//
// Returns a KCError instance when trying to append a string to a numeric
// record and when trying to append a string in read-only mode.
//
// If the append is successful, the method returns nil.
func (d *DB) Append(key, value string) error {
	if d.mode < WRITE {
		return KCError("The database was opened in read-only mode, you can't append strings to records")
	}
	if _, err := d.GetInt(key); err == nil {
		return KCError("The database doesn't support append a string to a numeric record")
	}
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	lKey := C.size_t(len(key))
	lValue := C.size_t(len(value))
	if C.kcdbappend(d.db, cKey, lKey, cValue, lValue) == 0 {
		errMsg := d.LastError()
		return KCError(fmt.Sprintf("Failed to append a string to a record: %s", errMsg))
	}
	return nil
}

// GetGob gets a record in the database by its key, decoding from gob format.
//
// Returns nil in case of success. In case of errors, it returns a KCError
// instance explaining what happened.
func (d *DB) GetGob(key string, e interface{}) error {
	data, err := d.Get(key)
	if err != nil {
		return err
	}
	buffer := bytes.NewBufferString(data)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(e); err != nil {
		return KCError(fmt.Sprintf("Failed to decode the record with the key %s: %s", key, err))
	}
	return nil
}

// SetGob adds a record to the database, stored in gob format.
//
// Returns a KCError instance in case of errors, otherwise, returns nil.
func (d *DB) SetGob(key string, e interface{}) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(e)
	if err != nil {
		return KCError(fmt.Sprintf("Failed to add a record with the value %s and the key %s: %s", e, key, err))
	}
	err = d.Set(key, buffer.String())
	return err
}

// Get retrieves a record in the database by its key.
//
// Returns the string value and nil in case of success, in case of errors,
// return a zero-valued string and an KCError instance (including when the key
// doesn't exist in the database).
func (d *DB) Get(key string) (string, error) {
	var resultLen C.size_t
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	cValue := C.kcdbget(d.db, cKey, lKey, &resultLen)
	defer C.free(unsafe.Pointer(cValue))
	if cValue == nil {
		errMsg := d.LastError()
		return "", KCError(fmt.Sprintf("Failed to get the record with the key %s: %s", key, errMsg))
	}
	return C.GoStringN(cValue, C.int(resultLen)), nil
}

// Remove removes a record from the database by its key.
//
// Returns a KCError instance if there is no record for the given key,
// or in case of other errors. The error instance contains a message
// describing what happened
func (d *DB) Remove(key string) error {
	if d.mode < WRITE {
		return KCError("The database was opened in read-only mode, you can't remove a record from it")
	}
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	status := C.kcdbremove(d.db, cKey, lKey)
	if status == 0 {
		errMsg := d.LastError()
		return KCError(fmt.Sprintf("Failed to remove the record with the key %s: %s", key, errMsg))
	}
	return nil
}

// Clear removes all records from the database.
//
// Returns a KCError in case of failure.
func (d *DB) Clear() error {
	if C.kcdbclear(d.db) == 0 {
		msg := d.LastError()
		return KCError(fmt.Sprintf("Failed to clear the database: %s.", msg))
	}
	return nil
}

// BeginTransaction begins a new transaction. It accepts a boolean flag that
// indicates whether the transaction should be hard or not. A hard transaction
// is a transaction that provides physical synchronization in the device, while
// a non-hard transaction provides logical synchronization with the file
// system.
func (d *DB) BeginTransaction(hard bool) error {
	var cHard C.int32_t = 0
	if hard {
		cHard = 1
	}
	if C.kcdbbegintran(d.db, cHard) == 0 {
		msg := d.LastError()
		return KCError(fmt.Sprintf("Could not begin transaction: %s.", msg))
	}
	return nil
}

// Commit commits the current transaction.
func (d *DB) Commit() error {
	if C.kcdbendtran(d.db, 1) == 0 {
		msg := d.LastError()
		return KCError(fmt.Sprintf("Could not commit the transaction: %s.", msg))
	}
	return nil
}

// Rollback aborts the current transaction.
func (d *DB) Rollback() error {
	if C.kcdbendtran(d.db, 0) == 0 {
		msg := d.LastError()
		return KCError(fmt.Sprintf("Could not rollback the transaction: %s.", msg))
	}
	return nil
}

// SetInt defines the value of an integer record, creating it when there is no
// record corresponding to the given key.
//
// Returns an KCError in case of errors setting the value.
func (d *DB) SetInt(key string, number int) error {
	if d.mode < WRITE {
		return KCError("SetInt doesn't work in read-only mode")
	}
	d.Remove(key)
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	cValue := C.int64_t(number)
	if C.kcdbincrint(d.db, cKey, lKey, cValue, 0) == C.INT64_MIN {
		errMsg := d.LastError()
		return KCError(fmt.Sprintf("Error setting integer value: %s", errMsg))
	}
	return nil
}

// GetInt gets a numeric record from the database.
//
// In case of errors (e.g.: when the given key refers to a non-numeric record),
// returns 0 and a KCError instance.
func (d *DB) GetInt(key string) (int, error) {
	v, err := d.Get(key)
	if err != nil {
		return 0, err
	} else if strings.IndexRune(v, '\x00') != 0 {
		return 0, KCError("Error: GetInt can't be used to get a non-numeric record")
	}
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	number := C.kcdbincrint(d.db, cKey, lKey, 0, 0)
	return int(number), nil
}

// Increment increments the value of a numeric record by a given number, and
// return the incremented value.
//
// In case of errors, returns 0 and a KCError instance with detailed error
// message.
func (d *DB) Increment(key string, number int) (int, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	cValue := C.int64_t(number)
	v := C.kcdbincrint(d.db, cKey, lKey, cValue, 0)
	if v == C.INT64_MIN {
		return 0, KCError("It's not possible to increment a non-numeric record")
	}
	return int(v), nil
}

// Count returns the number of records in the database.
func (d *DB) Count() (int, error) {
	var err error
	v := int(C.kcdbcount(d.db))
	if v == -1 {
		err = d.LastError()
	}
	return v, err
}

// CompareAndSwap performs a compare-and-swap operation, receiving three
// parameters: key, old and new.
//
// If the value corresponding to key is equal to old, then it is set to new. If
// the operation fails, this method returns a non-nil error.
func (d *DB) CompareAndSwap(key, old, new string) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	cOldValue := C.CString(old)
	defer C.free(unsafe.Pointer(cOldValue))
	lOldValue := C.size_t(len(old))
	cNewValue := C.CString(new)
	defer C.free(unsafe.Pointer(cNewValue))
	lNewValue := C.size_t(len(new))
	if C.kcdbcas(d.db, cKey, lKey, cOldValue, lOldValue, cNewValue, lNewValue) == 0 {
		return d.LastError()
	}
	return nil
}

// Close closes the database, make sure you always call this method after using
// the database.
//
// You can do it using the defer statement:
//
//     db := Open("my_db.kch", WRITE)
//     defer db.Close()
func (d *DB) Close() {
	C.kcdbclose(d.db)
	C.kcdbdel(d.db)
}
