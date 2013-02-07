// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package kc

import (
	"os"
)

func exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func remove(path string) {
	if exists(path) {
		os.Remove(path)
	}
}

func areStringMapsEqual(m1, m2 map[string]string) bool {
	for k, v := range m1 {
		if v != m2[k] {
			return false
		}
	}
	return true
}
