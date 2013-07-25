package kt

import (
	"os/exec"
	"reflect"
	"strconv"
	"testing"
	"time"
)

const (
	KTHOST = "127.0.0.1"
	KTPORT = 23034
)

func startServer(t *testing.T) *exec.Cmd {
	port := strconv.Itoa(KTPORT)
	cmd := exec.Command("ktserver", "-host", KTHOST, "-port", port, "%")

	if err := cmd.Start(); err != nil {
		t.Fatal("failed to start KT: ", err)
	}

	time.Sleep(5000000 * time.Nanosecond)
	return cmd
}

func haltServer(cmd *exec.Cmd, t *testing.T) {
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal("failed to halt KT: ", err)
	}
}

func TestOpenClose(t *testing.T) {

	cmd := startServer(t)
	defer haltServer(cmd, t)

	db, err := Open(KTHOST, KTPORT, DEFAULT_TIMEOUT)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
}

func TestCount(t *testing.T) {

	cmd := startServer(t)
	defer haltServer(cmd, t)

	db, err := Open(KTHOST, KTPORT, DEFAULT_TIMEOUT)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}

	db.Set("name", "Steve Vai")
	if n, err := db.Count(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Errorf("Count failed: want 1, got %d.", n)
	}
}

func TestGetSet(t *testing.T) {

	cmd := startServer(t)
	defer haltServer(cmd, t)

	db, err := Open(KTHOST, KTPORT, DEFAULT_TIMEOUT)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		db.Set(k, k)
		got, _ := db.Get(k)
		if got != k {
			t.Errorf("Get failed: want %s, got %s.", k, got)
		}
	}
}

func TestMatchPrefix(t *testing.T) {

	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := Open(KTHOST, KTPORT, DEFAULT_TIMEOUT)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{
		"cache/news/1",
		"cache/news/2",
		"cache/news/3",
		"cache/news/4",
	}
	for _, k := range keys {
		db.Set(k, "something")
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
		values, err := db.MatchPrefix(tt.prefix, tt.max)
		if err != nil && tt.expected != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(values, tt.expected) {
			t.Errorf("db.MatchPrefix(%q, 2). Want %#v. Got %#v.", tt.prefix, tt.expected, values)
		}
	}
}

func TestGetBulk(t *testing.T) {

	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := Open(KTHOST, KTPORT, DEFAULT_TIMEOUT)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
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
		db.Set(k, v)
		testKeys[k] = ""
	}

	err = db.GetBulk(testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	// Now remove some keys
	db.Remove("cache/news/1")
	db.Remove("cache/news/2")
	delete(baseKeys, "cache/news/1")
	delete(baseKeys, "cache/news/2")

	err = db.GetBulk(testKeys)
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

	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := Open(KTHOST, KTPORT, DEFAULT_TIMEOUT)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
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

	for k, _ := range (baseKeys) {
		testKeys[k] = ""
		removeKeys = append(removeKeys, k)
	}

	if _, err = db.SetBulk(baseKeys); err != nil {
		t.Fatal(err)
	}

	if err = db.GetBulk(testKeys); err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	if _, err = db.RemoveBulk(removeKeys); err != nil {
		t.Fatal(err)
	}

	count, _ := db.Count()
	if count != 0 {
		t.Errorf("db.RemoveBulk(). Want %v. Got %v", 0, count)
	}
}

func TestGetBulkBytes(t *testing.T) {

	cmd := startServer(t)
	defer haltServer(cmd, t)
	db, err := Open(KTHOST, KTPORT, DEFAULT_TIMEOUT)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
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
		db.Set(k, string(v))
		testKeys[k] = []byte("")
	}

	err = db.GetBulkBytes(testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulk(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	// Now remove some keys
	db.Remove("cache/news/4")
	delete(baseKeys, "cache/news/4")

	err = db.GetBulkBytes(testKeys)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range baseKeys {
		if !reflect.DeepEqual(v, testKeys[k]) {
			t.Errorf("db.GetBulkBytes(). Want %v. Got %v. for key %s", v, testKeys[k], k)
		}
	}

	if _, ok := testKeys["cache/news/4"]; ok {
		t.Errorf("db.GetBulkBytes(). Returned deleted key %v.", "cache/news/1")
	}
}
