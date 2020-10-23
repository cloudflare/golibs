package kt

import (
	"context"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const (
	KTHOST = "127.0.0.1"
	KTPORT = 23034
)

func startServerUnix(t testing.TB, sockAddr string) *exec.Cmd {
	db := "/tmp/test.rocksdb"

	cmd := exec.Command("qsutil", "db", "create", db)
	// This is a hack. As long as QS is running the file won't be removed by the kernel.
	defer os.RemoveAll(db)

	if err := cmd.Run(); err != nil {
		t.Fatal("failed to create QS DB: ", err)
	}

	cmd = exec.Command("qsdaemon", "--ktrpc=unix://"+sockAddr, "--cli=tcp4://localhost:4242", db)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Fatal("failed to start QS: ", err)
	}

	for i := 0; ; i++ {
		conn, err := net.Dial("tcp", "localhost:4242")
		if err == nil {
			break
			conn.Close()
		}
		time.Sleep(50 * time.Millisecond)
		if i > 50 {
			t.Fatal("failed to start QS: ", err)
		}
	}

	cmdW := exec.Command("qsutil", "cli", "set", "--server=tcp4://localhost:4242", "1", "2")
	if err := cmdW.Run(); err != nil {
		t.Fatal("failed to write to QS: ", err)
	}

	return cmd
}

func startServer(t testing.TB) *exec.Cmd {
	port := strconv.Itoa(KTPORT)

	if _, err := net.Dial("tcp", KTHOST+":"+port); err == nil {
		t.Fatal("Not expecting ktserver to exist yet. Perhaps: killall ktserver?")
	}

	cmd := exec.Command("ktserver", "-host", KTHOST, "-port", port, "%")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Fatal("failed to start KT: ", err)
	}

	for i := 0; ; i++ {
		conn, err := net.Dial("tcp", KTHOST+":"+port)
		if err == nil {
			conn.Close()
			return cmd
		}
		time.Sleep(50 * time.Millisecond)
		if i > 50 {
			t.Fatal("failed to start KT: ", err)
		}
	}
}

func haltServer(cmd *exec.Cmd, t testing.TB) {
	defer os.RemoveAll("/tmp/bad.sock")

	// QS forks a child for zero downtime upgrade so we need this hackery
	syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)

	if _, err := cmd.Process.Wait(); err != nil {
		t.Fatal("failed to halt KT: ", err)
	}
}

func TestCount(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)

	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}

	db.set(ctx, "name", []byte("Steve Vai"))
	if n, err := db.Count(ctx); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Errorf("Count failed: want 1, got %d.", n)
	}
}

func TestGetSet(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)

	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		db.set(ctx, k, []byte(k))
		got, _ := db.Get(ctx, k)
		if got != k {
			t.Errorf("Get failed: want %s, got %s.", k, got)
		}
	}
}

func TestMatchPrefix(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}

	keys := []string{
		"cache/news/1",
		"cache/news/2",
		"cache/news/3",
		"cache/news/4",
	}
	for _, k := range keys {
		db.set(ctx, k, []byte("something"))
	}
	var tests = []struct {
		max      int64
		prefix   string
		expected []string
	}{
		{
			max:      2,
			prefix:   "cache/news",
			expected: keys[:2],
		},
		{
			max:      10,
			prefix:   "cache/news",
			expected: keys,
		},
		{
			max:      10,
			prefix:   "/cache/news",
			expected: nil,
		},
	}
	for _, tt := range tests {
		values, err := db.MatchPrefix(ctx, tt.prefix, tt.max)
		if err != nil && tt.expected != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(values, tt.expected) {
			t.Errorf("db.MatchPrefix(%q, 2). Want %#v. Got %#v.", tt.prefix, tt.expected, values)
		}
	}

	values, err := db.MatchPrefix(ctx, "//////////DoNotExistAAAAAA", 1028)
	if len(values) != 0 || err != ErrSuccess {
		t.Errorf("db.MatchPrefix(DoNotExistAAAAAA, 1000). Want %d, got %d", len(values), err)
	}

	values, err = db.MatchPrefix(ctx, "//////////DoNotExistBBBBBB", 1028)
	if len(values) != 0 || err != ErrSuccess {
		t.Errorf("db.MatchPrefix(//////////DoNotExistBBBBBB, 1028). Want %d, got %d", len(values), err)
	}

	values, err = db.MatchPrefix(ctx, "c", 1028)
	if len(values) != 4 || err != nil {
		t.Errorf("db.MatchPrefix(//////////DoNotExistBBBBBB, 1028). Want %d, got %d", len(values), err)
	}
}

func TestGetBulk(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}

	testKeys := map[string]string{}
	baseKeys := map[string]string{
		"cache/news/1": "1",
		"cache/news/2": "2",
		"cache/news/3": "3",
		"cache/news/4": "4",
		"cache/news/5": "5",
		"cache/news/6": "6",
	}

	for k, v := range baseKeys {
		db.set(ctx, k, []byte(v))
		testKeys[k] = ""
	}

	err = db.GetBulk(ctx, testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	// Now remove some keys
	db.remove(ctx, "cache/news/1")
	db.remove(ctx, "cache/news/2")
	delete(baseKeys, "cache/news/1")
	delete(baseKeys, "cache/news/2")

	err = db.GetBulk(ctx, testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	if _, ok := testKeys["cache/news/1"]; ok {
		t.Errorf("db.GetBulk(). Returned deleted key %v.", "cache/news/1")
	}
}

func TestSetGetRemoveBulk(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}

	testKeys := map[string]string{}
	baseKeys := map[string]string{
		"cache/news/1": "1",
		"cache/news/2": "2",
		"cache/news/3": "3",
		"cache/news/4": "4",
		"cache/news/5": "5",
		"cache/news/6": "6",
	}
	removeKeys := make([]string, len(baseKeys))

	for k, _ := range baseKeys {
		testKeys[k] = ""
		removeKeys = append(removeKeys, k)
	}

	if _, err := db.setBulk(ctx, baseKeys); err != nil {
		t.Fatal(err)
	}

	if err := db.GetBulk(ctx, testKeys); err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	if _, err := db.removeBulk(ctx, removeKeys); err != nil {
		t.Fatal(err)
	}

	count, _ := db.Count(ctx)
	if count != 0 {
		t.Errorf("db.removeBulk(). Want %v. Got %v", 0, count)
	}
}

func TestGetBulkBytes(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}

	testKeys := map[string][]byte{}
	baseKeys := map[string][]byte{
		"cache/news/1": []byte("1"),
		"cache/news/2": []byte("2"),
		"cache/news/3": []byte("3"),
		"cache/news/4": []byte("4"),
		"cache/news/5": []byte("5"),
		"cache/news/6": []byte("6"),
	}

	for k, v := range baseKeys {
		db.set(ctx, k, v)
		testKeys[k] = []byte("")
	}

	err = db.GetBulkBytes(ctx, testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	// Now remove some keys
	db.remove(ctx, "cache/news/4")
	delete(baseKeys, "cache/news/4")

	err = db.GetBulkBytes(ctx, testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulkBytes(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	if _, ok := testKeys["cache/news/4"]; ok {
		t.Errorf("db.GetBulkBytes(). Returned deleted key %v.", "cache/news/4")
	}

	noKeys := map[string][]byte{
		"XXXcache/news/1": []byte(""),
		"XXXcache/news/2": []byte(""),
		"XXXcache/news/3": []byte(""),
		"XXXcache/news/4": []byte(""),
		"XXXcache/news/5": []byte(""),
		"XXXcache/news/6": []byte(""),
	}
	err = db.GetBulkBytes(ctx, noKeys)
	if err != nil {
		t.Fatal(err)
	}

	if len(noKeys) != 0 {
		t.Errorf("db.GetBulkBinary. Want %d, got %d", 0, len(noKeys))
	}
}

func TestGetBulkBytesLargeValue(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}

	testKeys := map[string][]byte{}
	baseKeys := map[string][]byte{
		"cache/news/1": []byte("v=spf1 mx a:alligator.org a:mailout11.intuit.com a:mailout12.intuit.com a:mailout13.intuit.com a:mailout14.intuit.com a:mailout21.intuit.com a:mailout22.intuit.com a:mailout23.intuit.com a:mailout24.intuit.com a:lvmailout01.intuit.com a:lvmailout02.intuit\" \".com a:lvmailout03.intuit.com a:lvmailappout10.intuit.com a:lvmailappout11.intuit.com a:lvmailappout12.intuit.com a:lvmailappout13.intuit.com a:lvmailappout20.intuit.com a:lvmailappout21.intuit.com a:lvmailappout22.intuit.com a:lvmailappout23.intuit.com a\" \":mailout1b.intuit.com a:mailout2b.intuit.com a:mailout3b.intuit.com a:mailout4b.intuit.com a:mailout101.intuit.com a:mailout102.intuit.com a:mailout103.intuit.com a:mailout104.intuit.com a:mailout201.intuit.com a:mailout202.intuit.com a:mailout203.intuit.\" \"com a:mailout204.intuit.com a:mailout4a.intuit.com a:mailout1a.intuit.com a:mailout2a.intuit.com a:mailout3a.intuit.com a:mailout5a.intuit.com ip4:209.251.131.160/28 ip4:206.154.105.172 ip4:206.154.105.173 ip4:206.154.105.174 ip4:206.154.105.175 ip4:206.1\" \"54.105.176 ip4:206.154.105.177 ip4:206.154.105.178 ip4:206.154.105.179 ip4:199.16.139.16 ip4:199.16.139.17 ip4:199.16.139.18 ip4:199.16.139.20 ip4:199.16.139.21 ip4:199.16.139.22 ip4:199.16.139.23 ip4:199.16.139.24 ip4:199.16.139.25 ip4:199.16.139.26 ip4:\" \"199.16.139.27 ip4:206.108.40.7 ip4:206.108.40.8 ip4:206.108.40.9 ip4:206.108.40.10 ip4:206.108.40.11 ip4:206.108.40.12 ip4:206.108.40.13 ip4:206.108.40.14 ip4:206.108.40.15 ip4:206.108.40.16 ip4:206.108.40.17 ip4:206.108.40.28 ip4:206.108.40.90 ip4:206.10\" \"8.40.91 ip4:206.108.40.92 include:_spf.google.com -all"),
		"cache/news/2": []byte("2"),
		"cache/news/3": []byte("3"),
		"cache/news/4": []byte("sdjkfhsdkfjhskdjfhskdhfksdf"),
		"cache/news/5": []byte("3826498237rsjdhfkjsdhfkjhsdjkfhsdjkf2893yrjascmzxbncmnzbxvsefuwie"),
		"cache/news/6": []byte("6"),
	}

	for k, v := range baseKeys {
		db.set(ctx, k, v)
		testKeys[k] = []byte("")
	}

	err = db.GetBulkBytes(ctx, testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	err = db.GetBulkBytes(ctx, make(map[string][]byte))
	if err != nil {
		t.Fatal(err)
	}

	wrong := make(map[string][]byte)
	wrong["/////doesntexitst"] = []byte("blah")

	err = db.GetBulkBytes(ctx, wrong)
	if err != nil {
		t.Fatal(err)
	}
	if len(wrong["/////doesntexitst"]) != 0 {
		t.Error(wrong["/////doesntexitst"])
	}
}

func TestGetBytes(t *testing.T) {
	ctx := context.Background()
	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := NewConn(KTHOST, KTPORT, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = db.GetBytes(ctx, "//doesntexist")
	if err != ErrNotFound {
		t.Fatal(err)
	}
}

func TestGetBytesUnix(t *testing.T) {
	sock := "/tmp/bad.sock"
	ctx := context.Background()
	cmd := startServerUnix(t, sock)

	defer haltServer(cmd, t)

	db, err := NewConn("unix://"+sock, 0, 1, DEFAULT_TIMEOUT)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.GetBytes(ctx, "//doesntexist")
	if err != ErrNotFound {
		t.Fatal(err)
	}
	v, err := db.GetBytes(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}

	if string(v) != "2" {
		t.Fatal("KV pair does not match")
	}
}

func TestIsError(t *testing.T) {
	err := &Error{Message: "What a hoopy frood"}
	if !IsError(err) {
		t.Error("IsError returns false")
	}
}
