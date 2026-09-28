package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18081", "listen address")
	name := flag.String("name", "local", "service name")
	flag.Parse()

	var flaky atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprintf(writer, `{"service":%q,"status":"ok"}`, *name)
	})
	mux.HandleFunc("/echo/", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(writer, "service=%s method=%s path=%s\n", *name, request.Method, request.URL.Path)
	})
	mux.HandleFunc("/flaky", func(writer http.ResponseWriter, request *http.Request) {
		attempt, _ := strconv.Atoi(request.Header.Get("X-Gateway-Attempt"))
		seen := flaky.Add(1)
		if attempt <= 1 || seen == 1 {
			http.Error(writer, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(writer, "recovered response\n")
	})
	mux.HandleFunc("/long", func(writer http.ResponseWriter, request *http.Request) {
		flusher := writer.(http.Flusher)
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusOK)
		fmt.Fprint(writer, "LONG-RESPONSE-BEGIN\n")
		flusher.Flush()
		for i := 0; ; i++ {
			select {
			case <-request.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			fmt.Fprintf(writer, "chunk-%06d\n", i)
			flusher.Flush()
		}
	})

	log.Printf("local service %s listening on %s", *name, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
