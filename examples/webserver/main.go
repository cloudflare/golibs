// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"github.com/fsouza/gokabinet/kc"
	"io"
	"log"
	"net/http"
)

type VisitsHandler struct {
	db *kc.DB
}

func (h VisitsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if visits, err := h.db.Increment("visits", 1); err == nil {
		io.WriteString(w, fmt.Sprintf("Welcome, you are the visitor number %d!", visits))
	} else {
		log.Fatal(err)
	}
}

func (h VisitsHandler) CloseDB() {
	h.db.Close()
}

func GetHandler() VisitsHandler {
	db, err := kc.Open("/tmp/visits.kch", kc.WRITE)
	if err != nil {
		log.Fatal(err)
	}
	return VisitsHandler{db: db}
}

func main() {
	h := GetHandler()
	defer h.CloseDB()
	http.Handle("/", h)
	if err := http.ListenAndServe(":6000", nil); err != nil {
		log.Fatal(err)
	}
}
