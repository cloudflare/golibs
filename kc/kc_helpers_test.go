// Copyright 2012 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kc

import (
	"os"
	"syscall"
)

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || err.(*os.PathError).Err != syscall.ENOENT
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
