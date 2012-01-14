package kc

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lkyotocabinet
# include <kclangc.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type KCError string

func (err KCError) Error() string {
	return string(err)
}

type DB struct {
	db *C.KCDB
}

func (d *DB) LastError() string {
	errMsg := C.GoString(C.kcecodename(C.kcdbecode(d.db)))
	return errMsg
}

func (d *DB) Set(key, value string) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	lKey := C.size_t(len(key))
	lValue := C.size_t(len(value))

	if (C.kcdbset(d.db, cKey, lKey, cValue, lValue) == 0) {
		errMsg := d.LastError()
		err := KCError(fmt.Sprintf("Failed to set the value %s to the key %s: %s", value, key, errMsg))
		return err
	}

	return nil
}

func (d *DB) Get(key string) (string, error) {
	var resultLen C.size_t

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	lKey := C.size_t(len(key))

	cValue := C.kcdbget(d.db, cKey, lKey, &resultLen)
	defer C.kcfree(unsafe.Pointer(cValue))

	if (cValue == nil) {
		errMsg := d.LastError()
		err := KCError(fmt.Sprintf("Failed to get a value for the key %s: %s", key, errMsg))
		return "", err
	}

	return C.GoString(cValue), nil
}

func (d *DB) Close() {
	C.kcfree(unsafe.Pointer(d.db))
}

func OpenForWrite(dbfilepath string) (*DB, error) {
	d := &DB{db: C.kcdbnew()}

	dbname := C.CString(dbfilepath)
	defer C.free(unsafe.Pointer(dbname))

	if (C.kcdbopen(d.db, dbname, C.KCOWRITER | C.KCOCREATE) == 0) {
		errMsg := d.LastError()
		err := KCError(fmt.Sprintf("Error opening %s: %s", dbfilepath, errMsg))
		return nil, err
	}

	return d, nil
}
