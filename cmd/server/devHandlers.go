package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

func (a *App) healthzHandler(writer http.ResponseWriter, r *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if r.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writer.WriteHeader(http.StatusOK)
	if _, err := writer.Write([]byte("ok\n")); err != nil {
		log.Printf("write response: %v", err)
	}
}

func (a *App) readyzHandler(writer http.ResponseWriter, r *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if r.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()

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

func (a *App) RunHandler(writer http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	switch r.Method {
	case http.MethodPost:
		{
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			runID, err := createRun(ctx, a.pool)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				if _, werr := fmt.Fprintf(writer, "Error during creating new run: %v\n", err); werr != nil {
					log.Printf("write response: %v", werr)
				}
				return
			}

			writer.WriteHeader(http.StatusCreated)
			if err := json.NewEncoder(writer).Encode(map[string]int{"run_id": runID}); err != nil {
				log.Printf("write response: %v", err)
			}
		}
	case http.MethodGet:
		{
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			idStr := r.URL.Query().Get("id")
			if idStr == "" {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "Id can't be empty"); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			runID, err := strconv.Atoi(idStr)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "Error during getting id: %v\n", err); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			if runID <= 0 {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "Id can't be negative number"); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}

			res := RunEntry{}
			if err := getRunByID(ctx, a.pool, runID, &res); err != nil {
				if err == pgx.ErrNoRows {
					writer.WriteHeader(http.StatusNotFound)
				} else {
					writer.WriteHeader(http.StatusInternalServerError)
				}
				if _, err := fmt.Fprintf(writer, "Error during getting entry from table: %v\n", err); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}

			writer.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(writer).Encode(res); err != nil {
				log.Printf("write response: %v", err)
			}
		}
	default:
		{
			writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			writer.WriteHeader(http.StatusMethodNotAllowed)
			if _, werr := fmt.Fprintf(writer, "Allow: \n%s\n%s\n", http.MethodGet, http.MethodPost); werr != nil {
				log.Printf("write response: %v", werr)
			}
			return
		}
	}
}

func (a *App) insertRowHandler(writer http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	strRunId := r.URL.Query().Get("run_id")
	runId, err := strconv.Atoi(strRunId)
	if err != nil || runId <= 0 {
		writer.WriteHeader(http.StatusBadRequest)
		if strRunId == "" {
			if _, werr := fmt.Fprintf(writer, "run_id is required"); werr != nil {
				log.Printf("write response: %v", werr)
			}
		} else {
			if _, werr := fmt.Fprintf(writer, "%s is not correct id", strRunId); werr != nil {
				log.Printf("write response: %v", werr)
			}
		}
		return
	}
	bodybytes, err := io.ReadAll(r.Body)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		if _, werr := fmt.Fprintf(writer, "Error during reading request body: %v", err); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(bodybytes, &payload); err != nil {
		payload = nil
		deadID, err := insertDeadLetter(ctx, a.pool, runId, string(bodybytes), err.Error(), payload)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			if _, werr := fmt.Fprintf(writer, "Error during inserting dead letter: %v", err); werr != nil {
				log.Printf("write response: %v", werr)
			}
			return
		}

		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(map[string]int{"dead_letter_id": deadID}); err != nil {
			log.Printf("write response: %v", err)
		}
		return
	}
	rowID, err := insertRow(ctx, a.pool, runId, payload)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		if _, werr := fmt.Fprintf(writer, "Error during inserting row: %v", err); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}
	writer.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(writer).Encode(map[string]int{"row_id": rowID}); err != nil {
		log.Printf("write response: %v", err)
	}

}
