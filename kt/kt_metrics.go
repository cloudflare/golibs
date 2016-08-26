package kt

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TrackedConn is a wrapper around kt.Conn that will accept a prometheus counter
// vector, and keep track of number of IO operations made to KT.
type TrackedConn struct {
	kt        *Conn
	opCounter *prometheus.CounterVec
	opTimer   *prometheus.SummaryVec
}

const (
	opCount        = "COUNT"
	opRemove       = "REMOVE"
	opGetBulk      = "GETBULK"
	opGet          = "GET"
	opGetBytes     = "GETBYTES"
	opSet          = "SET"
	opGetBulkBytes = "GETBULKBYTES"
	opSetBulk      = "SETBULK"
	opRemoveBulk   = "REMOVEBULK"
	opMatchPrefix  = "MATCHPREFIX"
)

// NewTrackedConn creates a new connection to a Kyoto Tycoon endpoint, and tracks
// operations made to it using prometheus metrics.
// All supported operations are tracked, opCounter is used to track the number of
// times each operation occurs, and opTimer times the number of nanoseconds each type
// of operation took, generating a summary.
func NewTrackedConn(host string, port int, poolsize int, timeout time.Duration,
	opCounter *prometheus.CounterVec, opTimer *prometheus.SummaryVec) (*TrackedConn, error) {
	conn, err := NewConn(host, port, poolsize, timeout)
	if err != nil {
		return nil, err
	}

	return &TrackedConn{
		kt:        conn,
		opCounter: opCounter,
		opTimer:   opTimer}, nil
}

func (c *TrackedConn) Count() (int, error) {
	c.opCounter.WithLabelValues(opGet).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opGet).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.Count()
}

func (c *TrackedConn) Remove(key string) error {
	c.opCounter.WithLabelValues(opRemove).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opRemove).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.Remove(key)
}

func (c *TrackedConn) GetBulk(keysAndVals map[string]string) error {
	c.opCounter.WithLabelValues(opGetBulk).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opGetBulk).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.GetBulk(keysAndVals)
}

func (c *TrackedConn) Get(key string) (string, error) {
	c.opCounter.WithLabelValues(opGet).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opGet).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.Get(key)
}

func (c *TrackedConn) GetBytes(key string) ([]byte, error) {
	c.opCounter.WithLabelValues(opGetBytes).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opGetBytes).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.GetBytes(key)
}

func (c *TrackedConn) Set(key string, value []byte) error {
	c.opCounter.WithLabelValues(opSet).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opSet).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.Set(key, value)
}

func (c *TrackedConn) GetBulkBytes(keys map[string][]byte) error {
	c.opCounter.WithLabelValues(opGetBulkBytes).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opGetBulkBytes).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.GetBulkBytes(keys)
}

func (c *TrackedConn) SetBulk(values map[string]string) (int64, error) {
	c.opCounter.WithLabelValues(opSetBulk).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opSetBulk).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.SetBulk(values)
}

func (c *TrackedConn) RemoveBulk(keys []string) (int64, error) {
	c.opCounter.WithLabelValues(opRemoveBulk).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opRemoveBulk).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.RemoveBulk(keys)
}

func (c *TrackedConn) MatchPrefix(key string, maxrecords int64) ([]string, error) {
	c.opCounter.WithLabelValues(opMatchPrefix).Inc()

	start := time.Now()
	defer func() {
		since := time.Since(start)
		c.opTimer.WithLabelValues(opMatchPrefix).Observe(float64(since.Nanoseconds()))
	}()

	return c.kt.MatchPrefix(key, maxrecords)
}
