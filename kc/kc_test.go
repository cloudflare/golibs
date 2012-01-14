package kc

import (
	"os"
	"testing"
)

func Exists(path string) bool {
	_, err := os.Stat(path)
	return err.(*os.PathError).Err != os.ENOENT
}

func TestShouldCreateTheFileOnDiscWhenOpenForWriting(t* testing.T) {
	filepath := "/tmp/names.kch"
	db, _ := OpenForWriting(filepath)
	defer db.Close()
	if !Exists(filepath) {
		t.Errorf("%s should exists, but it doesn't", filepath)
	}
}
