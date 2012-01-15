package kc

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lkyotocabinet
#include <kclangc.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const (
	_ = iota
	READ
	WRITE
)

// Type used for errors using the kabinet library.
// It implements the builting error interface.
type KCError string

func (err KCError) Error() string {
	return string(err)
}

// The basic type for the kabinet library. Holds an unexported instance
// of the database, for interactions.
type DB struct {
	db *C.KCDB
	mode int
}

// Returns a readable string to the last occurred error in the database
func (d *DB) LastError() string {
	errMsg := C.GoString(C.kcecodename(C.kcdbecode(d.db)))
	return errMsg
}

// Adds a record to the database. Currently, it's able to store only string values.
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
		err := KCError(fmt.Sprintf("Failed to add a record with the value %s and the key %s: %s", value, key, errMsg))
		return err
	}

	return nil
}

// Gets a record in the database by its key.
//
// Returns the string value and nil in case of success, in case of
// errors, return a zero-valued string and an KCError instance (including
// when the key doesn't exist in the database).
func (d *DB) Get(key string) (string, error) {
	var resultLen C.size_t

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	lKey := C.size_t(len(key))

	cValue := C.kcdbget(d.db, cKey, lKey, &resultLen)
	defer C.kcfree(unsafe.Pointer(cValue))

	if cValue == nil {
		errMsg := d.LastError()
		err := KCError(fmt.Sprintf("Failed to get the record with the key %s: %s", key, errMsg))
		return "", err
	}

	return C.GoString(cValue), nil
}

// Removes a record from the database by its key.
//
// Returns an error if the key is not found or other errors
// returns a KCError instance with a message describing the error
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
		err := KCError(fmt.Sprintf("Failed to remove the record with the key %s: %s", key, errMsg))
		return err
	}

	return nil
}

// Closes the database, make sure you always call this method after using the database.
//
// You can do it using the defer statement:
//
//     db := Open("my_db.kch", WRITE)
//     defer db.Close()
func (d *DB) Close() {
	C.kcdbclose(d.db)
	C.kcdbdel(d.db)
}

// Opens a database
// There are constants for the modes: READ and WRITE.
//
// The READ indicates read-only access to the database, the WRITE
// indicates read and write access to the database (there isn't a write-only mode)
func Open(dbfilepath string, mode int) (*DB, error) {
	d := &DB{db: C.kcdbnew(), mode: mode}

	dbname := C.CString(dbfilepath)
	defer C.free(unsafe.Pointer(dbname))

	cMode := C.uint32_t(C.KCOREADER)
	if mode > READ {
		cMode = C.KCOWRITER|C.KCOCREATE
	}

	if C.kcdbopen(d.db, dbname, cMode) == 0 {
		errMsg := d.LastError()
		err := KCError(fmt.Sprintf("Error opening %s: %s", dbfilepath, errMsg))
		return nil, err
	}

	return d, nil
}
