// Copyright 2012 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kc

import (
	"strings"
	"testing"
)

func TestShouldBeAbleToSetAndGetAStringRecord(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	if db, err := Open(filepath, WRITE); err == nil {
		defer db.Close()

		db.Set("name", "Alanis Morissette")
		if name, _ := db.Get("name"); name != "Alanis Morissette" {
			t.Errorf("Should add a record with the value \"Alanis Morissette\" and the key \"name\", but got \"%s\".", name)
		}
	} else {
		t.Errorf("Failed to open the file: %s.", filepath)
	}
}

func TestShouldReturnErrorExplainingWhenAStringRecordIsNotFound(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	if db, err := Open(filepath, WRITE); err == nil {
		defer db.Close()
		_, err := db.Get("name")
		if err == nil || !strings.Contains(err.Error(), "no record") {
			t.Errorf("Should return a clear error message when no record is found for a key.")
		}
	} else {
		t.Errorf("Failed to open the file: %s.", filepath)
	}
}

func TestShouldNotBeAbleToSetAStringRecordInREADMode(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	db, _ := Open(filepath, WRITE) // creating the file
	db.Close()
	db, _ = Open(filepath, READ)
	defer db.Close()
	err := db.Set("name", "Frank Sinatra")
	if err == nil || !strings.Contains(err.Error(), "read-only mode") {
		t.Errorf("It should not be possible to add a new record in read-only mode, but I was able to set")
	}
}

func TestShouldBeAbleToRemoveAStringRecordFromTheDatabase(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	if db, err := Open(filepath, WRITE); err == nil {
		db.Set("name", "Steve Vai")
		db.Set("instrument", "Guitar")
		db.Remove("instrument")
		_, err := db.Get("instrument")
		if err == nil {
			t.Errorf("The instrument value should be removed from the database, but it wasn't")
		}
	} else {
		t.Errorf("Failed to open file %s: %s", filepath, err.Error())
	}
}

func TestShouldReturnAnErrorMessageWhenTryingToRemoveANonPresentStringRecord(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	if db, err := Open(filepath, WRITE); err == nil {
		err := db.Remove("instrument")
		if err == nil || !strings.Contains(err.Error(), "no record") {
			t.Errorf("Should not be able to remove an non-present record from the database")
		}
	} else {
		t.Errorf("Failed to open file %s: %s", filepath, err.Error())
	}
}

func TestShouldBeAbleToAppendAStringToAStringRecord(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	if db, err := Open(filepath, WRITE); err == nil {
		db.Set("name", "Steve")
		db.Append("name", " Vai")
		if v, _ := db.Get("name"); v != "Steve Vai" {
			t.Errorf("The name value should be Steve Vai, but was %s", v)
		}
	} else {
		t.Errorf("Error opening %s: %s", filepath, err.Error())
	}
}

func TestShouldNotBeAbleToAppendAStringToAStringRecordInREADMode(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	db, _ := Open(filepath, WRITE)
	db.Set("name", "Steve")
	db.Close()
	db, _ = Open(filepath, READ)
	defer db.Close()
	if err := db.Append("name", " Vai"); err == nil || !strings.Contains(err.Error(), "read-only mode") {
		t.Errorf("Should not be able to append a string to a string record in read-only mode")
	}
}

func TestShouldNotBeAbleToAppendStringsToNumericRecords(t *testing.T) {
	filepath := "/tmp/musicians.kch"
	defer Remove(filepath)
	db, _ := Open(filepath, WRITE)
	defer db.Close()
	db.SetInt("age", 50)
	if err := db.Append("age", "50"); err == nil || !strings.Contains(err.Error(), "numeric record") {
		t.Errorf("Should not be able to append a string to a numeric record, and provide a descriptive message for the error")
	}
}
