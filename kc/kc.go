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

// ApplyFunc, function used as parameter to the Apply and AsyncApply
// methods. The function receives three parameters:
//
//		key: the record key
//		value: the record value
//		args: extra arguments passed to Apply method
type ApplyFunc func (key string, value interface{}, args ...interface{})

// Type used for errors using the gokabinet library.
// It implements the builting error interface.
type KCError string

func (err KCError) Error() string {
	return string(err)
}

// The basic type for the gokabinet library. Holds an unexported instance
// of the database, for interactions.
type DB struct {
	db   *C.KCDB
	filepath string
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

// This methods appends a string to the end of the value of a string
// record. It can't append any value to a numeric record.
//
// Returns a KCError instance when trying to append a string to a numeric
// record and when trying to append a string in read-only mode.
//
// If the append is successful, the method returns nil
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
	defer C.free(unsafe.Pointer(cValue))

	if cValue == nil {
		errMsg := d.LastError()
		err := KCError(fmt.Sprintf("Failed to get the record with the key %s: %s", key, errMsg))
		return "", err
	}

	return C.GoString(cValue), nil
}

// Removes a record from the database by its key.
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
		err := KCError(fmt.Sprintf("Failed to remove the record with the key %s: %s", key, errMsg))
		return err
	}

	return nil
}

// Sets the value of an integer record, creating it when there is no
// record correspondent to the given key
//
// Returns an KCError in case of errors setting the value
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

// Gets a numeric record from the database
//
// In case of errors (e.g.: when the given key refers to a non-numeric record),
// returns 0 and a KCError instance.
func (d *DB) GetInt(key string) (int, error) {
	v, err := d.Get(key)
	if err != nil {
		return 0, err
	} else if (v != "") {
		err := KCError("Error: don't use GetInt to get a non-numeric record")
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	lKey := C.size_t(len(key))
	number := C.kcdbincrint(d.db, cKey, lKey, 0, 0)

	return int(number), nil
}

// Increments the value of a numeric record by a given number,
// and return the incremented value
//
// In case of errors, returns 0 and a KCError instance with detailed
// error message
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

// Applies a funcion to all records in the database
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
	d := &DB{db: C.kcdbnew(), mode: mode, filepath: dbfilepath}

	dbname := C.CString(dbfilepath)
	defer C.free(unsafe.Pointer(dbname))

	cMode := C.uint32_t(C.KCOREADER)
	if mode > READ {
		cMode = C.KCOWRITER | C.KCOCREATE
	}

	if C.kcdbopen(d.db, dbname, cMode) == 0 {
		errMsg := d.LastError()
		err := KCError(fmt.Sprintf("Error opening %s: %s", dbfilepath, errMsg))
		return nil, err
	}

	return d, nil
}
