package kt

import (
	"strconv"
	"testing"
)

func BenchmarkInsert(b *testing.B) {
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		conn.Set(str, []byte(str))
	}
}
