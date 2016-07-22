package kt

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
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

// KT has 2 interfaces, A restful one and an RPC one.
// The RESTful interface is usually much faster than
// the RPC one, but not all methods are implemented.
// Use the RESTFUL interfaces when we can and fallback
// to the RPC one when needed.
//
// The RPC format uses tab separated values with a choice of encoding
// for each of the fields. We use base64 since it is always safe.
//
// REST format is just the body of the HTTP request being the value.

// NewConn creates a connection to an Kyoto Tycoon endpoint.
func NewConn(host string, port int, poolsize int, timeout time.Duration) (*Conn, error) {
	portstr := strconv.Itoa(port)
	c := &Conn{
		timeout: timeout,
		host:    net.JoinHostPort(host, portstr),
		transport: &http.Transport{
			ResponseHeaderTimeout: timeout,
			MaxIdleConnsPerHost:   poolsize,
		},
	}

	// connectivity check so that we can bail out
	// early instead of when we do the first operation.
	_, _, err := c.doRPC("/rpc/void", "", nil)
	if err != nil {
		return nil, err
	}
	return c, nil
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
	code, m, err := c.doRPC("/rpc/status", "", nil)
	if err != nil {
		return 0, err
	}
	if code != 200 {
		return 0, makeError(m)
	}
	return strconv.Atoi(string(findRec(m, "count").value))
}

// Remove deletes the data at key in the database.
func (c *Conn) Remove(key string) error {
	code, body, err := c.doREST("DELETE", key, nil)
	if err != nil {
		return err
	}
	if code == 404 {
		return ErrNotFound
	}
	if code != 204 {
		return errors.New(string(body))
	}
	return nil
}

// GetBulk retrieves the keys in the map. The results will be filled in on function return.
// If a key was not found in the database, it will be removed from the map.
// Note that this is NOT atomic. If you want to make an atomic call, use the
// GetBulkAtomic API.
func (c *Conn) GetBulk(keysAndVals map[string]string) error {
	return c.GetBulkAtomic(keysAndVals, false)
}

// GetBulk retrieves the keys in the map. The results will be filled in on function return.
// If a key was not found in the database, it will be removed from the map.
func (c *Conn) GetBulkAtomic(keysAndVals map[string]string, atomic bool) error {
	m := make(map[string][]byte)
	for k := range keysAndVals {
		m[k] = zeroslice
	}
	err := c.GetBulkBytesAtomic(m, atomic)
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
// ErrNotFound is returned if no such data is found.
func (c *Conn) GetBytes(key string) ([]byte, error) {
	code, body, err := c.doREST("GET", key, nil)
	if err != nil {
		return nil, err
	}
	switch code {
	case 200:
		break
	case 404:
		return nil, ErrNotFound
	default:
		return nil, errors.New(string(body))
	}
	return body, nil

}

// Set stores the data at key
func (c *Conn) Set(key string, value []byte) error {
	code, body, err := c.doREST("PUT", key, value)
	if err != nil {
		return err
	}
	if code != 201 {
		return errors.New(string(body))
	}

	return nil
}

var zeroslice = []byte("0")

// GetBulkBytes retrieves the keys in the map. The results will be filled in on function return.
// If a key was not found in the database, it will be removed from the map.
// Note that this is NOT atomic. If you want to make an atomic call, use the
// GetBulkBytesAtomic API.
func (c *Conn) GetBulkBytes(keys map[string][]byte) error {
	return c.GetBulkBytesAtomic(keys, false)
}

// GetBulkBytes retrieves the keys in the map. The results will be filled in on function return.
// If a key was not found in the database, it will be removed from the map.
func (c *Conn) GetBulkBytesAtomic(keys map[string][]byte, atomic bool) error {

	// The format for querying multiple keys in KT is to send a
	// TSV value for each key with a _ as a prefix.
	// KT then returns the value as a TSV set with _ in front of the keys
	keystransmit := make([]kv, 0, len(keys))
	for k, _ := range keys {
		// we set the value to nil because we want a sentinel value
		// for when no data was found. This is important for
		// when we remove the not found keys from the map
		keys[k] = nil
		keystransmit = append(keystransmit, kv{"_" + k, zeroslice})
	}

	var query string
	if atomic {
		query = "atomic=true"
	}

	code, m, err := c.doRPC("/rpc/get_bulk", query, keystransmit)
	if err != nil {
		return err
	}
	if code != 200 {
		return makeError(m)
	}
	for _, kv := range m {
		if kv.key[0] != '_' {
			continue
		}
		keys[kv.key[1:]] = kv.value
	}
	for k, v := range keys {
		if v == nil {
			delete(keys, k)
		}
	}
	return nil
}

// SetBulk stores the values in the map
// Note that this is NOT atomic. If you want to make an atomic call, use the
// SetBulkAtomic API.
func (c *Conn) SetBulk(values map[string]string) (int64, error) {
	return c.SetBulkAtomic(values, false)
}

// SetBulk stores the values in the map, either atomically, or not.
func (c *Conn) SetBulkAtomic(values map[string]string, atomic bool) (int64, error) {
	vals := make([]kv, 0, len(values))
	for k, v := range values {
		vals = append(vals, kv{"_" + k, []byte(v)})

	}

	var query string
	if atomic {
		query = "atomic=true"
	}

	code, m, err := c.doRPC("/rpc/set_bulk", query, vals)
	if err != nil {
		return 0, err
	}
	if code != 200 {
		return 0, makeError(m)
	}
	return strconv.ParseInt(string(findRec(m, "num").value), 10, 64)
}

// RemoveBulk deletes the values
// Note that this is NOT atomic. If you want to make an atomic call, use the
// RemoveBulkAtomic API.
func (c *Conn) RemoveBulk(keys []string) (int64, error) {
	return c.RemoveBulkAtomic(keys, false)
}

// RemoveBulk deletes the values, either atomically, or not.
func (c *Conn) RemoveBulkAtomic(keys []string, atomic bool) (int64, error) {
	vals := make([]kv, 0, len(keys))
	for _, k := range keys {
		vals = append(vals, kv{"_" + k, zeroslice})
	}

	var query string
	if atomic {
		query = "atomic=true"
	}

	code, m, err := c.doRPC("/rpc/remove_bulk", query, vals)
	if err != nil {
		return 0, err
	}
	if code != 200 {
		return 0, makeError(m)
	}
	return strconv.ParseInt(string(findRec(m, "num").value), 10, 64)
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
	code, m, err := c.doRPC("/rpc/match_prefix", "", keystransmit)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, makeError(m)
	}
	res := make([]string, 0, len(m))
	for _, kv := range m {
		if kv.key[0] == '_' {
			res = append(res, string(kv.key[1:]))
		}
	}
	if len(res) == 0 {
		// yeah, gokabinet was weird here.
		return nil, ErrSuccess
	}
	return res, nil
}

var base64headers http.Header
var identityheaders http.Header

func init() {
	identityheaders = make(http.Header)
	identityheaders.Set("Content-Type", "text/tab-separated-values")
	base64headers = make(http.Header)
	base64headers.Set("Content-Type", "text/tab-separated-values; colenc=B")
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
func (c *Conn) doRPC(path, query string, values []kv) (code int, vals []kv, err error) {
	url := &url.URL{
		Scheme:   "http",
		Host:     c.host,
		Path:     path,
		RawQuery: query,
	}
	req := &http.Request{
		Method: "POST",
		URL:    url,
	}
	body, enc := tsvEncode(values)
	req.Header = identityheaders
	if enc == base64Enc {
		req.Header = base64headers
	}

	bodyReader := bytes.NewBuffer(body)
	req.Body = ioutil.NopCloser(bodyReader)
	req.ContentLength = int64(len(body))
	t := time.AfterFunc(c.timeout, func() {
		c.transport.CancelRequest(req)
	})
	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		if !t.Stop() {
			err = ErrTimeout
		}
		return 0, nil, err
	}
	resultBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if !t.Stop() {
		return 0, nil, ErrTimeout
	}
	if err != nil {
		return 0, nil, err
	}
	m := decodeValues(resultBody, resp.Header.Get("Content-Type"))
	return resp.StatusCode, m, nil
}

type encoding int

const (
	identityEnc encoding = iota
	base64Enc
)

// Encode the request body in TSV. The encoding is chosen based
// on whether there are any binary data in the key/values
func tsvEncode(values []kv) ([]byte, encoding) {
	var bufsize int
	var hasbinary bool
	for _, kv := range values {
		// length of key
		hasbinary = hasbinary || hasBinary(kv.key)
		bufsize += base64.StdEncoding.EncodedLen(len(kv.key))
		// tab
		bufsize += 1
		// value
		hasbinary = hasbinary || hasBinarySlice(kv.value)
		bufsize += base64.StdEncoding.EncodedLen(len(kv.value))
		// newline
		bufsize += 1
	}
	buf := make([]byte, bufsize)
	var n int
	for _, kv := range values {
		if hasbinary {
			base64.StdEncoding.Encode(buf[n:], []byte(kv.key))
			n += base64.StdEncoding.EncodedLen(len(kv.key))
		} else {
			n += copy(buf[n:], kv.key)
		}
		buf[n] = '\t'
		n++
		if hasbinary {
			base64.StdEncoding.Encode(buf[n:], kv.value)
			n += base64.StdEncoding.EncodedLen(len(kv.value))
		} else {
			n += copy(buf[n:], kv.value)
		}
		buf[n] = '\n'
		n++
	}
	enc := identityEnc
	if hasbinary {
		enc = base64Enc
	}
	return buf, enc
}

func hasBinary(b string) bool {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < 0x20 || c > 0x7e {
			return true
		}
	}
	return false
}

func hasBinarySlice(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return true
		}
	}
	return false
}

// decodeValues takes a response from an KT RPC call and turns it into the
// values that it returned.
//
// KT can return values in 3 different formats, Tab separated values (TSV) without any field encoding,
// TSV with fields base64 encoded or TSV with URL encoding.
// KT does not give you any option as to the format that it returns, so we have to implement all of them
func decodeValues(buf []byte, contenttype string) []kv {
	if len(buf) == 0 {
		return nil
	}
	// Ideally, we should parse the mime media type here,
	// but this is an expensive operation because mime is just
	// that awful.
	//
	// KT responses are pretty simple and we can rely
	// on it putting the parameter of colenc=[BU] at
	// the end of the string. Just look for B, U or s
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

	// Because of the encoding, we can tell how many records there
	// are by scanning through the input and counting the \n's
	var recCount int
	for _, v := range buf {
		if v == '\n' {
			recCount++
		}
	}
	result := make([]kv, 0, recCount)
	b := bytes.NewBuffer(buf)
	for {
		key, err := b.ReadBytes('\t')
		if err != nil {
			return result
		}
		key = decodef(key[:len(key)-1])
		value, err := b.ReadBytes('\n')
		if len(value) > 0 {
			fieldlen := len(value) - 1
			if value[len(value)-1] != '\n' {
				fieldlen = len(value)
			}
			value = decodef(value[:fieldlen])
			result = append(result, kv{string(key), value})
		}
		if err != nil {
			return result
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
func makeError(m []kv) error {
	kv := findRec(m, "ERROR")
	if kv.key == "" {
		return errors.New("kt: generic error")
	}
	return errors.New("kt: " + string(kv.value))
}

func findRec(kvs []kv, key string) kv {
	for _, kv := range kvs {
		if kv.key == key {
			return kv
		}
	}
	return kv{}
}

// empty header for REST calls.
var emptyHeader = make(http.Header)

func (c *Conn) doREST(op string, key string, val []byte) (code int, body []byte, err error) {
	newkey := urlenc(key)
	url := &url.URL{
		Scheme: "http",
		Host:   c.host,
		Opaque: newkey,
	}
	req := &http.Request{
		Method: op,
		URL:    url,
		Header: emptyHeader,
	}
	req.ContentLength = int64(len(val))
	req.Body = ioutil.NopCloser(bytes.NewBuffer(val))
	t := time.AfterFunc(c.timeout, func() {
		c.transport.CancelRequest(req)
	})
	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		if !t.Stop() {
			err = ErrTimeout
		}
		return 0, nil, err
	}
	resultBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if !t.Stop() {
		err = ErrTimeout
	}
	return resp.StatusCode, resultBody, err
}

// encode the key for use in a RESTFUL url
// KT requires that we use URL escaped values for
// anything not safe in a query component.
// Add a slash for the leading slash in the url.
func urlenc(s string) string {
	return "/" + url.QueryEscape(s)
}
