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
	"bytes"
	"fmt"
	"unsafe"
	"encoding/gob"
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
		valforprint := value
		truncatedots := ""
		if len(value) > 80 {
			valforprint = value[:80]
			truncatedots = "..."
		}
		return KTError(fmt.Sprintf("Failed to add a record with the value %q%s and the key %q: %s", valforprint, truncatedots, key, errMsg))
	}
	return nil
}

// Get byte[] directly
func (d *RemoteDB) GetBytes(key string) ([]byte, error) {
	var resultLen C.size_t
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	cValue := C.ktdbget(d.db, cKey, lKey, &resultLen)
	defer C.free(unsafe.Pointer(cValue))
	if cValue == nil {
		errMsg := d.LastError()
		return []byte{}, KTError(fmt.Sprintf("Failed to get the record with the key %s: %s", key, errMsg))
	}
	return C.GoBytes(unsafe.Pointer(cValue), C.int(resultLen)), nil
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

// GetGob gets a record in the database by its key, decoding from gob format.
//
// Returns nil in case of success. In case of errors, it returns a KCError
// instance explaining what happened.
func (d *RemoteDB) GetGob(key string, e interface{}) error {
	data, err := d.Get(key)
	if err != nil {
		return err
	}
	buffer := bytes.NewBufferString(data)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(e); err != nil {
		return KTError(fmt.Sprintf("Failed to decode the record with the key %s: %s", key, err))
	}
	return nil
}

// SetGob adds a record to the database, stored in gob format.
//
// Returns a KCError instance in case of errors, otherwise, returns nil.
func (d *RemoteDB) SetGob(key string, e interface{}) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(e)
	if err != nil {
		return KTError(fmt.Sprintf("Failed to add a record with the value %s and the key %s: %s", e, key, err))
	}
	err = d.Set(key, buffer.String())
	return err
}

// Removes the key from the DB.
func (d *RemoteDB) Remove(key string) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	status := C.ktdbremove(d.db, cKey, lKey)
	if status == 0 {
		errMsg := d.LastError()
		return KTError(fmt.Sprintf("Failed to remove the record with the key %s: %s", key, errMsg))
	}
	return nil
}

// Clear removes all records from the database.
//
// Returns a KTError in case of failure.
func (d *RemoteDB) Clear() error {
	if C.ktdbclear(d.db) == 0 {
		msg := d.LastError()
		return KTError(fmt.Sprintf("Failed to clear the database: %s.", msg))
	}
	return nil
}

// Increment increments the value of a numeric record by a given number, and
// return the incremented value.
//
// In case of errors, returns 0 and a KCError instance with detailed error
// message.
func (d *RemoteDB) Increment(key string, number int) (int, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	lKey := C.size_t(len(key))
	cValue := C.int64_t(number)
	v := C.ktdbincrint(d.db, cKey, lKey, cValue, 0)
	if v == C.INT64_MIN {
		return 0, KTError("It's not possible to increment a non-numeric record")
	}
	return int(v), nil
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

// MatchPrefix returns a list of keys that matches a prefix or an error in case
// of failure.
func (d *RemoteDB) MatchPrefix(prefix string, max int64) ([]string, error) {
	cprefix := C.CString(prefix)
	defer C.free(unsafe.Pointer(cprefix))
	strary := C.match_prefix(d.db, cprefix, C.size_t(max))
	if strary.v == nil {
		return nil, d.LastError()
	}
	defer C.free_strary(&strary)
	n := int64(strary.n)
	if n == 0 {
		return nil, nil
	}
	result := make([]string, n)
	for i := int64(0); i < n; i++ {
		result[i] = C.GoString(C.strary_item(&strary, C.int64_t(i)))
	}
	return result, nil
}

// GetBulk fetches all keys in the given map. If a key does not exist, it is deleted from the map before being returned.
// If the key does exist, the value in the map is set accordingly.
func (d *RemoteDB) GetBulk(keysAndVals map[string]string) (error) {

	keyList := make([]string, len(keysAndVals))
	cKeys := C.make_char_array(C.int(len(keysAndVals)))
	defer C.free_char_array(cKeys, C.int(len(keysAndVals)))
	next := 0;
	for s, _ := range (keysAndVals) {
        C.set_array_string(cKeys, C.CString(s), C.int(next))
		keyList[next] = s
		next++
	}

	strary := C.get_bulk_binary(d.db, cKeys, C.size_t(len(keysAndVals)))
	if strary.v == nil {
		return d.LastError()
	}
	defer C.free_strary(&strary)
	n := int64(strary.n)
	if n == 0 {
		return nil
	}

	for i := int64(0); i < n; i++ {
		if C.bool(C.strary_present(&strary, C.int64_t(i))) {
			keysAndVals[keyList[i]] = C.GoString(C.strary_item(&strary, C.int64_t(i)))
		} else {
			delete(keysAndVals, keyList[i])
		}
	}
	return nil
}

// GetBulkBytes fetches all keys in the given map. If a key does not exist, it is deleted from the map before being returned.
// If the key does exist, the value in the map is set accordingly.
func (d *RemoteDB) GetBulkBytes(keysAndVals map[string][]byte) (error) {

	keyList := make([]string, len(keysAndVals))
	cKeys := C.make_char_array(C.int(len(keysAndVals)))
	defer C.free_char_array(cKeys, C.int(len(keysAndVals)))
	next := 0;
	for s, _ := range (keysAndVals) {
        C.set_array_string(cKeys, C.CString(s), C.int(next))
		keyList[next] = s
		next++
	}

	strary := C.get_bulk_binary(d.db, cKeys, C.size_t(len(keysAndVals)))
	if strary.v == nil {
		return d.LastError()
	}
	defer C.free_strary(&strary)
	n := int64(strary.n)
	if n == 0 {
		return nil
	}

	for i := int64(0); i < n; i++ {
		if C.bool(C.strary_present(&strary, C.int64_t(i))) {
			keysAndVals[keyList[i]] = C.GoBytes(unsafe.Pointer(C.strary_item(&strary, C.int64_t(i))), C.int(C.strary_size(&strary, C.int64_t(i))))
		} else {
			delete(keysAndVals, keyList[i])
		}
	}
	return nil
}

// RemoveBulk removes all of the keys passed in at once.
func (d *RemoteDB) RemoveBulk(keys []string) (int64, error) {

	var err error
	cKeys := C.make_char_array(C.int(len(keys)))
	defer C.free_char_array(cKeys, C.int(len(keys)))
	next := 0;
	for _, s := range (keys) {
        C.set_array_string(cKeys, C.CString(s), C.int(next))
		next++
	}

	n := int64(C.ktdbremovebulkbinary(d.db, cKeys, C.size_t(len(keys))))
	if n == -1 {
		err = d.LastError()
	}
	return n, err
}

// SetBulk sets all of the keys passed in at once.
func (d *RemoteDB) SetBulk(keysAndVals map[string]string) (int64, error) {

	var err error
	cKeys := C.make_char_array(C.int(len(keysAndVals)))
	cVals := C.make_char_array(C.int(len(keysAndVals)))
	defer C.free_char_array(cKeys, C.int(len(keysAndVals)))
	defer C.free_char_array(cVals, C.int(len(keysAndVals)))
	next := 0;
	for k, v := range (keysAndVals) {
        C.set_array_string(cKeys, C.CString(k), C.int(next))
        C.set_array_string(cVals, C.CString(v), C.int(next))
		next++
	}

	n := int64(C.ktdbsetbulkbinary(d.db, cKeys, C.size_t(len(keysAndVals)), cVals, C.size_t(len(keysAndVals))))
	if n == -1 {
		err = d.LastError()
	}
	return n, err
}

// Plays the passed in lua script, returning the result as a key->value map.
func (d *RemoteDB) PlayScript(script string, params map[string]string) (map[string]string, error) {

	result := map[string]string{}
	cScript := C.CString(script)
	defer C.free(unsafe.Pointer(cScript))

	paramSize := len(params) * 2
	cParams := C.make_char_array(C.int(paramSize))
	defer C.free_char_array(cParams, C.int(paramSize))
	next := 0;
	for k, v := range (params) {
        C.set_array_string(cParams, C.CString(k), C.int(next))
		next++
        C.set_array_string(cParams, C.CString(v), C.int(next))
		next++
	}

	strary := C.play_script(d.db, cScript, cParams, C.size_t(paramSize))
	if strary.v == nil {
		return nil, d.LastError()
	}
	defer C.free_strary(&strary)
	n := int64(strary.n)
	if n == 0 {
		return result, nil
	}

	for i := int64(0); i < n; i++ {
		result[C.GoString(C.strary_item(&strary, C.int64_t(i)))] = C.GoString(C.strary_item(&strary, C.int64_t(i+1)))
		i++
	}
	return result, nil
}