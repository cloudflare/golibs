package kc

import (
	"os"
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

func areMapsEqual(m1, m2 map[string]string) bool {
	for k, v := range m1 {
		if v != m2[k] {
			return false
		}
	}
	return true
}
