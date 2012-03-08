/**
* Example from README
**/
package main

import (
	"github.com/fsouza/gokabinet/kc"
	"log"
)

func main() {
	db, err := kc.Open("/tmp/cache.kch", kc.WRITE)

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	db.Set("names", "Maria|João|José")
	db.SetInt("hits", 500)

	for i := 0; i < 100; i++ {
		db.Increment("hits", 1)
	}
}
