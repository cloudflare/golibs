// Copyright 2012 gokabinet authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kc

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

func TestShouldCreateTheFileInTheDiscWhenOpenForReadAndWrite(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer remove(filepath)

	db, _ := Open(filepath, WRITE)
	defer db.Close()

	if !exists(filepath) {
		t.Errorf("%s should exists, but it doesn't", filepath)
	}
}

func TestShouldHoldTheFilePathInTheDBObject(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer remove(filepath)
	db, _ := Open(filepath, WRITE)
	defer db.Close()
	if db.Path != filepath {
		t.Errorf("The filepath should be %s, but was %s", filepath, db.Path)
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
	filepath := "/tmp/musicians.kch"
	defer remove(filepath)
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

func TestCount(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer remove(filepath)
	db, _ := Open(filepath, WRITE)
	defer db.Close()
	if n, err := db.Count(); err != nil {
		t.Error(err)
		t.FailNow()
	} else if n != 0 {
		t.Errorf("Count failed: want 0, got %d.", n)
		t.FailNow()
	}
	db.Set("name", "Steve Vai")
	if n, err := db.Count(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Errorf("Count failed: want 1, got %d.", n)
	}
}

func TestShouldHaveConstantsForReadAndWrite(t *testing.T) {
	if READ != 1 || WRITE != 2 {
		t.Errorf("constant READ should be 1 and WRITE should be 2")
	}
}

func TestShouldNotBeAbleToRemoveARecordInReadOnlyMode(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer remove(filepath)
	db, _ := Open(filepath, WRITE)
	db.Close()
	db, _ = Open(filepath, READ)
	defer db.Close()
	err := db.Remove("instrument")
	if err == nil || !strings.Contains(err.Error(), "read-only mode") {
		t.Errorf("Should not be able to remove a record in read-only mode")
	}
}

func BenchmarkSetAndGet(b *testing.B) {
	keys := []string{}
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
						keys = append(keys, keyAndValue[0])
					}
				}

				for _, key := range keys {
					db.Get(key)
				}
			}
		}
	}
}
