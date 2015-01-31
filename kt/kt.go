// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package kt

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
)

const DEFAULT_TIMEOUT = 2 * time.Second

// Conn represents a connection to a kyoto tycoon endpoint.
// It uses a connection pool to efficiently communicate with the server.
// Conn is safe for concurrent use.
type Conn struct {
	timeout   time.Duration
	host      string
	transport *http.Transport
}

// KT supports a "RESTful" interface that is cheaper than the TSV-RPC,
// We use the TSV interface because the RESTful interface is underspecified.
// It is not clear how you are to escape the URLs in a couple of cases, specifically
// around the use of slash as a key. Since we might have to escape the slashes inside
// a path, that means using the opaque URL field, which I really don't like

// NewConn creates a connection to an Kyoto Tycoon endpoint.
func NewConn(host string, port int, poolsize int, timeout time.Duration) *Conn {
	portstr := strconv.Itoa(port)
	c := &Conn{
		timeout: timeout,
		host:    net.JoinHostPort(host, portstr),
		transport: &http.Transport{
			ResponseHeaderTimeout: timeout,
			MaxIdleConnsPerHost:   poolsize,
		},
	}

	return c
}

var (
	ErrTimeout = errors.New("kt: operation timeout")
	// the wording on this error is deliberately weird,
	// because users would search for the string logical inconsistency
	// in order to find lookup misses.
	ErrNotFound = errors.New("kt: entry not found aka logical inconsistency")
	// old gokabinet returned this error on success. Keeping around "for compatibility" until
	// I can kill it with fire.
	ErrSuccess = errors.New("kt: success")
)

// Count returns the number of records in the database
func (c *Conn) Count() (int, error) {
	code, m, err := c.doRPC("/rpc/status", nil)
	if err != nil {
		return 0, err
	}
	if code != 200 {
		return 0, makeError(m)
	}
	return strconv.Atoi(string(m["count"]))
}

// Remove deletes the data at key in the database.
func (c *Conn) Remove(key string) error {
	vals := []kv{{"key", []byte(key)}}
	code, m, err := c.doRPC("/rpc/remove", vals)
	if err != nil {
		return err
	}
	if code != 200 {
		return makeError(m)
	}
	return nil
}

// GetBulk retrieves the keys in the map. The results will be filled in on function return.
// If a key was not found in the database, it will be removed from the map.
func (c *Conn) GetBulk(keysAndVals map[string]string) error {
	m := make(map[string][]byte)
	for k := range keysAndVals {
		m[k] = zeroslice
	}
	err := c.GetBulkBytes(m)
	if err != nil {
		return err
	}
	for k := range keysAndVals {
		b, ok := m[k]
		if ok {
			keysAndVals[k] = string(b)
		} else {
			delete(keysAndVals, k)
		}
	}
	return nil
}

// Get retrieves the data stored at key. ErrNotFound is
// returned if no such data exists
func (c *Conn) Get(key string) (string, error) {
	s, err := c.GetBytes(key)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

// GetBytes retrieves the data stored at key in the format of a byte slice
// A nil slice and nil error is returned if no data at key exists.
func (c *Conn) GetBytes(key string) ([]byte, error) {
	vals := []kv{{"key", []byte(key)}}
	code, m, err := c.doRPC("/rpc/get", vals)
	if err != nil {
		return nil, err
	}
	switch code {
	case 200:
		break
	case 450:
		return nil, ErrNotFound
	default:
		return nil, makeError(m)
	}
	return m["value"], nil

}

// Set stores the data at key
func (c *Conn) Set(key string, value []byte) error {
	vals := []kv{
		{"key", []byte(key)},
		{"value", value},
	}
	code, m, err := c.doRPC("/rpc/set", vals)
	if err != nil {
		return err
	}
	switch code {
	case 200:
		return nil
	default:
		return makeError(m)
	}
}

var zeroslice = []byte("0")

// GetBulkBytes retrieves the keys in the map. The results will be filled in on function return.
// If a key was not found in the database, it will be removed from the map.
func (c *Conn) GetBulkBytes(keys map[string][]byte) error {
	keystransmit := make([]kv, 0, len(keys))
	for k, _ := range keys {
		keystransmit = append(keystransmit, kv{"_" + k, zeroslice})
	}
	code, m, err := c.doRPC("/rpc/get_bulk", keystransmit)
	if err != nil {
		return err
	}
	if code != 200 {
		return makeError(m)
	}
	for k := range keys {
		val, ok := m["_"+k]
		if !ok {
			delete(keys, k)
			continue
		}
		keys[k] = val
	}
	return nil
}

// SetBulk stores the values in the map.
func (c *Conn) SetBulk(values map[string]string) (int64, error) {
	vals := make([]kv, 0, len(values))
	for k, v := range values {
		vals = append(vals, kv{"_" + k, []byte(v)})
	}
	code, m, err := c.doRPC("/rpc/set_bulk", vals)
	if err != nil {
		return 0, err
	}
	if code != 200 {
		return 0, makeError(m)
	}
	return strconv.ParseInt(string(m["num"]), 10, 64)
}

// RemoveBulk deletes the values
func (c *Conn) RemoveBulk(keys []string) (int64, error) {
	vals := make([]kv, 0, len(keys))
	for _, k := range keys {
		vals = append(vals, kv{"_" + k, zeroslice})
	}
	code, m, err := c.doRPC("/rpc/remove_bulk", vals)
	if err != nil {
		return 0, err
	}
	if code != 200 {
		return 0, makeError(m)
	}
	return strconv.ParseInt(string(m["num"]), 10, 64)
}

// MatchPrefix performs the match_prefix operation against the server
// It returns a sorted list of strings.
// The error may be ErrSuccess in the case that no records were found.
// This is for compatibility with the old gokabinet library.
func (c *Conn) MatchPrefix(key string, maxrecords int64) ([]string, error) {
	keystransmit := []kv{
		{"prefix", []byte(key)},
		{"max", []byte(strconv.FormatInt(maxrecords, 10))},
	}
	code, m, err := c.doRPC("/rpc/match_prefix", keystransmit)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, makeError(m)
	}
	res := make([]string, 0, len(m))
	for k := range m {
		if k[0] == '_' {
			res = append(res, string(k[1:]))
		}
	}
	if len(res) == 0 {
		// yeah, gokabinet was weird here.
		return nil, ErrSuccess
	}
	// kt spits the prefixes out in sorted order
	// so do that. Users depend on the order.
	sort.StringSlice(res).Sort()
	return res, nil
}

// prefabHeader is the header that rpc request share.
var prefabHeader = makeprefab()

func makeprefab() http.Header {
	r := make(http.Header)
	r.Set("Content-Type", "text/tab-separated-values; colenc=B")
	return r
}

// we use an explicit structure here rather than a map[string][]byte
// because for some operations, we care about the order
// and since we only care about direct key lookup in a few
// cases where the sets are small, we can amortize the cost of the map
type kv struct {
	key   string
	value []byte
}

// Do an RPC call against the KT endpoint.
func (c *Conn) doRPC(path string, values []kv) (code int, vals map[string][]byte, err error) {
	url := &url.URL{
		Scheme: "http",
		Host:   c.host,
		Path:   path,
	}
	req := &http.Request{
		Method: "POST",
		URL:    url,
		Header: prefabHeader,
	}
	body := tsvEncode(values)

	bodyReader := bytes.NewBuffer(body)
	req.Body = ioutil.NopCloser(bodyReader)
	req.ContentLength = int64(len(body))
	t := time.Now()
	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		return 0, nil, err
	}
	dur := time.Since(t)
	timeout := c.timeout - dur
	if timeout < 0 {
		return 0, nil, ErrTimeout
	}
	resultBody, err := timeoutRead(resp.Body, timeout)
	if err != nil {
		return 0, nil, err
	}
	m := decodeValues(resultBody, resp.Header.Get("Content-Type"))
	return resp.StatusCode, m, nil
}

// Encode the request body in base64 encoded TSV
func tsvEncode(values []kv) []byte {
	var bufsize int
	for _, kv := range values {
		// length of key
		bufsize += base64.StdEncoding.EncodedLen(len(kv.key))
		// tab
		bufsize += 1
		// value
		bufsize += base64.StdEncoding.EncodedLen(len(kv.value))
		// newline
		bufsize += 1
	}
	buf := make([]byte, bufsize)
	var n int
	for _, kv := range values {
		base64.StdEncoding.Encode(buf[n:], []byte(kv.key))
		n += base64.StdEncoding.EncodedLen(len(kv.key))
		buf[n] = '\t'
		n++
		base64.StdEncoding.Encode(buf[n:], kv.value)
		n += base64.StdEncoding.EncodedLen(len(kv.value))
		buf[n] = '\n'
		n++
	}
	return buf
}

func timeoutRead(body io.ReadCloser, timeout time.Duration) ([]byte, error) {
	t := time.AfterFunc(timeout, func() {
		body.Close()
	})
	buf, err := ioutil.ReadAll(body)
	if t.Stop() {
		body.Close()
	} else {
		err = ErrTimeout
	}
	return buf, err
}

// decodeValues takes a response from an KT RPC call and turns it into the
// values that it returned.
//
// KT can return values in 3 different formats, Tab separated values (TSV) without any field encoding,
// TSV with fields base64 encoded or TSV with URL encoding.
// KT does not give you any option as to the format that it returns, so we have to implement all of them
func decodeValues(buf []byte, contenttype string) map[string][]byte {
	if len(buf) == 0 {
		return nil
	}
	// Ideally, we should parse the mime media type here,
	// but this is an expensive operation because mime is just
	// that awful.
	//
	// KT responses are pretty simple and we can rely
	// on it putting the parameter of colenc=[BU] at
	// the end of the string. Just look for B, U or S
	// (last character of tab-separated-values)
	// to figure out which field encoding is used.
	var decodef decodefunc
	switch contenttype[len(contenttype)-1] {
	case 'B':
		decodef = base64Decode
	case 'U':
		decodef = urlDecode
	case 's':
		decodef = identityDecode
	default:
		panic("kt responded with unknown content-type: " + contenttype)
	}

	kv := make(map[string][]byte)
	b := bytes.NewBuffer(buf)
	for {
		key, err := b.ReadBytes('\t')
		if err != nil {
			return kv
		}
		key = decodef(key[:len(key)-1])
		value, err := b.ReadBytes('\n')
		if len(value) > 0 {
			fieldlen := len(value) - 1
			if value[len(value)-1] != '\n' {
				fieldlen = len(value)
			}
			value = decodef(value[:fieldlen])
			kv[string(key)] = value
		}
		if err != nil {
			return kv
		}
	}
}

// decodefunc takes a byte slice and decodes the
// value in place. It returns a slice pointing into
// the original byte slice. It is used for decoding the
// individual fields of the TSV that kt returns
type decodefunc func([]byte) []byte

// Don't do anything, this is pure TSV
func identityDecode(b []byte) []byte {
	return b
}

// Base64 decode each of the field
func base64Decode(b []byte) []byte {
	n, _ := base64.StdEncoding.Decode(b, b)
	return b[:n]
}

// Decode % escaped URL format
func urlDecode(b []byte) []byte {
	res := b
	resi := 0
	for i := 0; i < len(b); i++ {
		if b[i] != '%' {
			res[resi] = b[i]
			resi++
			continue
		}
		res[resi] = unhex(b[i+1])<<4 | unhex(b[i+2])
		resi++
		i += 2
	}
	return res[:resi]
}

// copied from net/url
func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

// TODO: make this return errors that can be introspected more easily
// and make it trim components of the error to filter out unused information.
func makeError(m map[string][]byte) error {
	return errors.New(string(m["ERROR"]))
}
