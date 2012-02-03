package kc

import (
	"testing"
)

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

func TestShouldBeAbleToAsynchronouslyApplyAFunctionToAllRecordsInTheDatabase(t *testing.T) {
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

	r := db.AsyncApply(func (key string, value interface{}, args ...interface{}) {
		applied[key] = value.(string)
	})
	r.Wait()

	if !areMapsEqual(urls, applied) {
		t.Errorf("Should apply the function")
	}
}
