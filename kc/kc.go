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

func (d *DB) Close() {
	C.kcfree(unsafe.Pointer(d.db))
}

func OpenForWriting(dbfilepath string) (*DB, error) {
	db := &DB{db: C.kcdbnew()}

	dbname := C.CString(dbfilepath)
	defer C.free(unsafe.Pointer(dbname))

	if (C.kcdbopen(db.db, dbname, C.KCOWRITER | C.KCOCREATE) == 0) {
		errMsg := C.GoString(C.kcecodename(C.kcdbecode(db.db)))
		err := KCError(fmt.Sprintf("Error opening the %s file: %s", dbfilepath, errMsg))
		return nil, err
	}

	return db, nil
}
