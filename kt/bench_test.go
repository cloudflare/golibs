package kt

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	ctx := context.Background()
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		b.Fatal(err.Error())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		conn.set(ctx, str, []byte(str))
	}
}

func BenchmarkSetLarge(b *testing.B) {
	ctx := context.Background()
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		b.Fatal(err.Error())
	}
	var large [4096]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		conn.set(ctx, str, large[:])
	}
}

func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		b.Fatal(err.Error())
	}
	err = conn.set(ctx, "something", []byte("foobar"))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Get(ctx, "something")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetLarge(b *testing.B) {
	ctx := context.Background()
	cmd := startServer(b)
	defer haltServer(cmd, b)
	conn, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		b.Fatal(err.Error())
	}
	err = conn.set(ctx, "something", make([]byte, 4096))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Get(ctx, "something")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBulkBytes(b *testing.B) {
	ctx := context.Background()
	cmd := startServer(b)
	defer haltServer(cmd, b)
	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		b.Fatal(err.Error())
	}

	keys := make(map[string][]byte)
	for i := 0; i < 200; i++ {
		keys[fmt.Sprintf("cache/news/%d", i)] = []byte{'4'}
	}

	for k := range keys {
		db.set(ctx, k, []byte("something"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.GetBulkBytes(ctx, keys)
		if err != nil {
			b.Fatal(err)
		}
	}
}
