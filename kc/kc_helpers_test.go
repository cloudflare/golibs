// Copyright 2012 gokabinet authors. All rights reserved.
// Use of this source code is governed by a BSD-style
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
