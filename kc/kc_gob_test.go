// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package kc

import (
	"testing"
)

func TestShouldBeAbleToSetAndGetANativeGoType(t *testing.T) {
	if db, err := Open("-", WRITE); err == nil {
		defer db.Close()

		data := make(map[string]int)
		data["one"] = 1
		data["two"] = 2

		db.SetGob("numbers", data)

		var numbers map[string]int
		err := db.GetGob("numbers", &numbers)
		if err != nil {
			t.Errorf("Failed to get gob: %s", err)
		}
		if numbers["one"] != 1 || numbers["two"] != 2 {
			t.Error("Should transparently persist complex types")
		}
	} else {
		t.Error("Failed to open a prototype hash database")
	}
}
