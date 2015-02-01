package kt

import (
	"strconv"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		conn.Set(str, []byte(str))
	}
}

func BenchmarkSetLarge(b *testing.B) {
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	var large [4096]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		conn.Set(str, large[:])
	}
}

func BenchmarkGet(b *testing.B) {
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	err := conn.Set("something", []byte("foobar"))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Get("something")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetLarge(b *testing.B) {
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	err := conn.Set("something", make([]byte, 4096))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Get("something")
		if err != nil {
			b.Fatal(err)
		}
	}
}
