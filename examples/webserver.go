package main

import (
	"github.com/fsouza/gokabinet/kc"
	"io"
	"log"
	"net/http"
)

type VisitsHandler struct {
	db *kc.DB
}

func (h *VisitsHandler) ServeHTTP(w http.ResponseWriter, r *Request) {
	h.db.Increment("visits", 1)
	visits := h.db.GetInt("visits")

	io.WriteString(w, fmt.Sprintf("Welcome, you are the %d visitor!", visits))
}

func main() {
	h := Handler{db: kc.Open("/tmp/visits.kch", kc.WRITE)}
	http.Handle("/", h)
	if err := http.ListenAndServe(":6060", nil); err != nil {
		log.Fatal(err)
	}
}
