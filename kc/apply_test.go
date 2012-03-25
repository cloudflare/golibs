// Copyright 2012 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kc

import (
	"io/ioutil"
	"strings"
	"testing"
)

func TestShouldBeAbleToApplyAFunctionToEachRecordsInTheDatabase(t *testing.T) {
	urls := map[string]string{
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

	db.Apply(func(key string, value interface{}, args ...interface{}) {
		applied[key] = value.(string)
	})

	if !areStringMapsEqual(urls, applied) {
		t.Errorf("Should apply the function")
	}
}

func TestShouldBeAbleToApplyAfunctiontoEachRecordInTheDatabaseWithExtraArguments(t *testing.T) {
	urls := map[string]string{
		"jsoP": "http://weekly.golang.org/",
		"9odk": "http://groups.google.com/group/golang-nuts",
		"cbrt": "http://cobrateam.info",
		"SpLR": "http://splinter.cobrateam.info",
	}

	expected := map[string]string{}
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

	db.Apply(func(key string, value interface{}, args ...interface{}) {
		var extraString string
		for _, a := range args {
			extraString += a.(string)
		}

		applied[key] = value.(string) + extraString
	}, "extra1", "extra2")

	if !areStringMapsEqual(expected, applied) {
		t.Errorf("Should apply the function with extra arguments")
	}
}

func TestShouldBeAbleToAsynchronouslyApplyAFunctionToAllRecordsInTheDatabase(t *testing.T) {
	urls := map[string]string{
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

	r := db.AsyncApply(func(key string, value interface{}, args ...interface{}) {
		applied[key] = value.(string)
	})
	r.Wait()

	if !areStringMapsEqual(urls, applied) {
		t.Errorf("Should apply the function")
	}
}

func BenchmarkSetAndApply(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if c, err := ioutil.ReadFile("testdata/urls.txt"); err == nil {
			s := string(c)
			filepath := "/tmp/shorturs.kch"
			if db, err := Open(filepath, WRITE); err == nil {
				parts := strings.Split(s, "\n")
				for _, part := range parts {
					keyAndValue := strings.Split(part, "\t")
					if len(keyAndValue) > 1 {
						db.Set(keyAndValue[0], keyAndValue[1])
					}
				}

				applied := make(map[string]string)
				toApply := func(key string, value interface{}, args ...interface{}) {
					applied[key] = value.(string)
				}

				db.Apply(toApply)
				db.Close()
			}
		}
	}
}
