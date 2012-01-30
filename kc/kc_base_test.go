package kc

import (
	"fmt"
	"strings"
	"testing"
)

func TestShouldCreateTheFileInTheDiscWhenOpenForReadAndWrite(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)

	db, _ := Open(filepath, WRITE)
	defer db.Close()

	if !Exists(filepath) {
		t.Errorf("%s should exists, but it doesn't", filepath)
	}
}

func TestShouldHoldTheFilePathInTheDBObject(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)

	db, _ := Open(filepath, WRITE)
	defer db.Close()

	if db.filepath != filepath {
		t.Errorf("The filepath should be %s, but was %s", filepath, db.filepath)
	}
}

func TestShouldReportADescriptiveErrorMessageWhenFailToOpenADatabaseForWrite(t *testing.T) {
	filepath := "/root/db.kch" // i won't be able to write here :)
	expectedMessagePart := fmt.Sprintf("Error opening %s:", filepath)

	_, err := Open(filepath, WRITE)

	if err == nil || !strings.Contains(err.Error(), expectedMessagePart) {
		t.Errorf("Should fail with a descriptive message")
	}
}

func TestShouldBeAbleToSetCloseOpenAgainAndReadInWriteMode(t *testing.T) {
	filepath := "/tmp/musicias.kch"
	defer Remove(filepath)

	db, _ := Open(filepath, WRITE)
	db.Set("name", "Steve Vai")
	db.Close()

	db, _ = Open(filepath, WRITE)
	defer db.Close()
	name, _ := db.Get("name")

	if name != "Steve Vai" {
		t.Errorf("Should be able to write, close, open and get the record stored in write mode")
	}
}

func TestShouldHaveConstantsForReadAndWrite(t *testing.T) {
	if READ != 1 || WRITE != 2 {
		t.Errorf("constant READ should be 1 and WRITE should be 2")
	}
}

func TestShouldNotBeAbleToRemoveARecordInReadOnlyMode(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)

	db, _ := Open(filepath, WRITE)
	db.Close()

	db, _ = Open(filepath, READ)
	defer db.Close()

	err := db.Remove("instrument")
	if err == nil || !strings.Contains(err.Error(), "read-only mode") {
		t.Errorf("Should not be able to remove a record in read-only mode")
	}
}

func TestShouldBeAbleToApplyAFunctionToEachRecordsInTheDatabase(t *testing.T) {
	urls := map[string]string {
		"jsoP": "http://weekly.golang.org/",
		"9odk": "http://groups.google.com/group/golang-nuts",
		"cbrt": "http://cobrateam.info",
		"SpLR": "http://splinter.cobrateam.info",
	}

	filepath := "/tmp/shorturls.kch"
	defer Remove(filepath)

	db, _ := Open(filepath, WRITE)
	defer db.Close()

	for k, v := range urls {
		db.Set(k, v)
	}

	applied := map[string]string{}

	db.Apply(func (key string, value interface{}, args ...interface{}) {
		applied[key] = value.(string)
	})

	if !areMapsEqual(urls, applied) {
		t.Errorf("Should apply the function")
	}
}

func TestShouldBeAbleToApplyAfunctiontoEachRecordInTheDatabaseWithExtraArguments(t *testing.T) {
	urls := map[string]string {
		"jsoP": "http://weekly.golang.org/",
		"9odk": "http://groups.google.com/group/golang-nuts",
		"cbrt": "http://cobrateam.info",
		"SpLR": "http://splinter.cobrateam.info",
	}

	expected := map[string]string {}
	for k, v := range urls {
		expected[k] = v + "extra1" + "extra2"
	}

	filepath := "/tmp/shorturls.kch"
	defer Remove(filepath)

	db, _ := Open(filepath, WRITE)
	defer db.Close()

	for k, v := range urls {
		db.Set(k, v)
	}

	applied := map[string]string{}

	db.Apply(func (key string, value interface{}, args ...interface{}) {
		var extraString string
		for _, a := range args {
			extraString += a.(string)
		}

		applied[key] = value.(string) + extraString
	}, "extra1", "extra2")

	if !areMapsEqual(expected, applied) {
		t.Errorf("Should apply the function with extra arguments")
	}
}
