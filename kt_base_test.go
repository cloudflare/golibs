package kt

import (
	"testing"
	"os/exec"
	"strconv"
	"time"
)

const (
	KTHOST = "127.0.0.1"
	KTPORT = 23034
	)

func startServer(t *testing.T) (*exec.Cmd) {
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
	for _,k := range(keys) {
		db.Set(k, k)
		got, _ := db.Get(k)
		if (got != k) {
			t.Errorf("Get failed: want %s, got %s.", k, got)
		}
	}
}
