package kc

import (
	"os"
	"testing"
)

func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || err.(*os.PathError).Err != os.ENOENT
}

func Remove(path string) {
	if Exists(path) {
		os.Remove(path)
	}
}

func TestShouldCreateTheFileOnDiscWhenOpenForWrite(t* testing.T) {
	filepath := "/tmp/names.kch"
	defer Remove(filepath)

	db, _ := OpenForWrite(filepath)
	defer db.Close()

	if !Exists(filepath) {
		t.Errorf("%s should exists, but it doesn't", filepath)
	}
}
