package main

import (
	"log"
	"net/http"
	"os"
	"time"
)

func getenv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func main() {
	serverAddr := getenv("ETL_HTTP_ADDR", ":8080")
	mux := http.NewServeMux()
	server := http.Server{
		Addr:              serverAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	mux.HandleFunc("/healthz", healthzHandler)
	log.Printf("Starting on %s", serverAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Fatal error: %v", err)
	}
	log.Printf("Server stopped")
}

func healthzHandler(writer http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if _, err := writer.Write([]byte("ok\n")); err != nil {
		log.Printf("write response: %v", err)
	}
}
