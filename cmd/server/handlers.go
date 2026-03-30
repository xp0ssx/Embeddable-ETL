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
			val := a.pool.QueryRow(ctx, `
				INSERT INTO runs DEFAULT VALUES
				RETURNING id;
				`)
			id := -1
			if err := val.Scan(&id); err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				if _, err := fmt.Fprintf(writer, "Error during getting id: %v\n", err); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			writer.WriteHeader(http.StatusCreated)
			if err := json.NewEncoder(writer).Encode(map[string]int{"run_id": id}); err != nil {
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
			id, err := strconv.Atoi(idStr)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "Error during getting id: %v\n", err); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			if id <= 0 {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "Id can't be negative number"); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}

			row := a.pool.QueryRow(ctx, `
				SELECT id, created_at, finished_at, status, total_rows, ok_rows, bad_rows FROM runs
				WHERE id = ($1)
				`, id)

			res := RunEntry{}
			if err := row.Scan(&res.Id, &res.Created_at, &res.Finished_at, &res.Status, &res.Total_rows, &res.Ok_rows, &res.Bad_rows); err != nil {
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
		row := a.pool.QueryRow(ctx, `
		INSERT INTO etl_dead_letters(run_id, raw_line, error, payload)
		VALUES ($1, $2, $3, NULL)
		RETURNING id
		`, runId, string(bodybytes), err.Error())

		rowId := -1
		if err := row.Scan(&rowId); err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			if _, werr := fmt.Fprintf(writer, "Error during inserting payload into table etl_dead_letters: %v", err); werr != nil {
				log.Printf("write response: %v", werr)
			}
			return
		}

		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(map[string]int{"dead_letter_id": rowId}); err != nil {
			log.Printf("write response: %v", err)
		}
		return
	}
	row := a.pool.QueryRow(ctx, `
	INSERT INTO etl_rows(run_id, payload)
	VALUES ($1, $2)
	RETURNING id;
	`, runId, payload)
	rowId := -1
	if err := row.Scan(&rowId); err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		if _, werr := fmt.Fprintf(writer, "Error during inserting payload into table etl_rows: %v", err); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}

	writer.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(writer).Encode(map[string]int{"row_id": rowId}); err != nil {
		log.Printf("write response: %v", err)
	}

}
