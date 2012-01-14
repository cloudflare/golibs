package kc

import (
	"fmt"
	"os"
	"strings"
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

func TestShouldCreateTheFileInTheDiscWhenOpenForWrite(t* testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)

	db, _ := OpenForWrite(filepath)
	defer db.Close()

	if !Exists(filepath) {
		t.Errorf("%s should exists, but it doesn't", filepath)
	}
}

func TestShouldReportADescriptiveErrorMessageWhenFailToOpenADatabaseForWrite(t *testing.T) {
	filepath := "/root/db.kch" // i won't be able to write here :)
	expectedMessagePart := fmt.Sprintf("Error opening %s:", filepath)

	_, err := OpenForWrite(filepath)

	if (err == nil || !strings.Contains(err.Error(), expectedMessagePart)) {
		t.Errorf("Should fail with a descriptive message")
	}
}

func TestShouldBeAbleToSetAndGetAValue(t* testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)

	if db, err := OpenForWrite(filepath); err == nil {
		defer db.Close()

		db.Set("name", "Alanis Morissette")
		if name, _ := db.Get("name"); name != "Alanis Morissette" {
			t.Errorf("Should set the value \"Alanis Morissette\" to the key \"name\", but got \"%s\".", name)
		}
	} else {
		t.Errorf("Failed to open the file: %s.", filepath)
	}
}

func TestShouldReturnErrorExplainingWhenAKeyIsNotFound(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)

	if db, err := OpenForWrite(filepath); err == nil {
		defer db.Close()

		_, err := db.Get("name")
		if err == nil || !strings.Contains(err.Error(), "no record"){
			t.Errorf("Should return a clear error message when no record is found for a key.")
		}
	} else {
		t.Errorf("Failed to open the file: %s.", filepath)
	}
}
