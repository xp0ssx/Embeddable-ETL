package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	pool *pgxpool.Pool
}

func main() {
	serverAddr := os.Getenv("ETL_HTTP_PORT")
	if serverAddr == "" {
		serverAddr = "8080"
	}
	serverAddr = ":" + serverAddr
	migrationsDir := os.Getenv("ETL_MIGRATIONS_DIR")
	if migrationsDir == "" {
		migrationsDir = "migrations"
	}

	dbDSN := os.Getenv("ETL_DB_DSN")
	if dbDSN == "" {
		log.Fatalf("Fatal error: no data for DB connection")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM) //Контекст завершения от докера
	defer stop()

	mctx, mcancel := context.WithTimeout(ctx, 30*time.Second)
	defer mcancel()

	pool, err := pgxpool.New(context.Background(), dbDSN)
	if err != nil {
		log.Fatalf("Fatal error: %v", err)
	}
	defer pool.Close()
	if err := applyMigrations(mctx, pool, migrationsDir); err != nil {
		log.Fatalf("Fatal error: %v", err)
	}
	app := &App{pool: pool}

	mux := http.NewServeMux()
	server := http.Server{
		Addr:              serverAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	mux.HandleFunc("/healthz", app.healthzHandler)
	mux.HandleFunc("/readyz", app.readyzHandler)

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("Starting on %s", serverAddr)
		serverErr <- server.ListenAndServe()
	}()

	var listenErr error

	select {
	case <-ctx.Done():
		log.Printf("Shutting down...")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("Shutdown error: %v", err)
		}

		listenErr = <-serverErr

	case listenErr = <-serverErr:
	}

	if listenErr != nil && listenErr != http.ErrServerClosed {
		log.Printf("Server stopped with error: %v", listenErr)
	}
	log.Printf("Server stopped")
}

func (a *App) healthzHandler(writer http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	if _, err := writer.Write([]byte("ok\n")); err != nil {
		log.Printf("write response: %v", err)
	}
}

func (a *App) readyzHandler(writer http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()

	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")

	if err := a.pool.Ping(ctx); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		if _, werr := fmt.Fprintf(writer, "DB error: %v\n", err); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}
	writer.WriteHeader(http.StatusOK)
	if _, err := writer.Write([]byte("ok\n")); err != nil {
		log.Printf("write response: %v", err)
	}
}
