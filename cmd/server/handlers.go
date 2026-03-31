package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
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

func (a *App) ingestCSVHandler(writer http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	runID := -1
	var err error
	idStr := r.URL.Query().Get("run_id")
	if idStr == "" {
		runID, err = createRun(ctx, a.pool)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			if _, werr := fmt.Fprintf(writer, "Error during creating new run: %v\n", err); werr != nil {
				log.Printf("write response: %v", werr)
			}
			return
		}
	} else {
		runID, err = strconv.Atoi(idStr)
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
	}

	reader := csv.NewReader(r.Body)
	header, err := reader.Read()
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		if _, werr := fmt.Fprintf(writer, "Error during reading header: %v\n", err); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}
	numCol := len(header)
	badRows, okRows, totalRows := 0, 0, 0
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			writer.WriteHeader(http.StatusInternalServerError)
			if _, werr := fmt.Fprintf(writer, "Error during reading csv: %v\n", err); werr != nil {
				log.Printf("write response: %v", werr)
			}
			return
		}
		totalRows++
		payload := map[string]any{}
		if len(record) != numCol {
			if _, err := insertDeadLetter(ctx, a.pool, runID, strings.Join(record, ","), "wrong number of columns", payload); err != nil {
				log.Printf("Error during inserting Dead Letter with runID %d: %v\n", runID, err)
			}
			badRows++
		} else {
			for i, colName := range header {
				payload[colName] = record[i]
			}
			if _, err := insertRow(ctx, a.pool, runID, payload); err != nil {
				if _, err := insertDeadLetter(ctx, a.pool, runID, strings.Join(record, ","), err.Error(), payload); err != nil {
					log.Printf("Error during inserting Dead Letter with runID %d: %v\n", runID, err)
				}
				badRows++
			} else {
				okRows++
			}
		}
	}

	if err := updateRunStats(ctx, a.pool, runID, totalRows, okRows, badRows); err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		if _, werr := fmt.Fprintf(writer, "Error during updating run entry %d: %v\n", runID, err); werr != nil {
			log.Printf("write response: %v", werr)
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
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(writer).Encode(res); err != nil {
		log.Printf("write response: %v", err)
	}
}

func (a *App) pipelinesHandler(writer http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPost:
		{
			entry := PipelineRequest{}
			if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "invalid json: %v", err); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			if len(entry.Schema) == 0 {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "schema is required"); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			if pipelineID, err := insertPipeline(ctx, a.pool, entry.Name, entry.Schema); err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				if _, err := fmt.Fprintf(writer, "Error during inserting pipeline: %v", err); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			} else {
				writer.Header().Set("Content-Type", "application/json; charset=utf-8")
				writer.WriteHeader(http.StatusCreated)
				if err := json.NewEncoder(writer).Encode(map[string]int{"pipeline_id": pipelineID}); err != nil {
					log.Printf("write response: %v", err)
				}
			}
		}
	case http.MethodGet:
		{
			idStr := r.URL.Query().Get("pipeline_id")
			if idStr == "" {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "Id is required"); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			var (
				pipelineID int
				err        error
			)
			if pipelineID, err = strconv.Atoi(idStr); err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				if _, err := fmt.Fprintf(writer, "Error during getting id"); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			pipeline := Pipeline{}
			if err := getPipelineByID(ctx, a.pool, pipelineID, &pipeline); err != nil {
				if err == pgx.ErrNoRows {
					writer.WriteHeader(http.StatusNotFound)
				} else {
					writer.WriteHeader(http.StatusInternalServerError)
				}

				if _, err := fmt.Fprintf(writer, "Error during getting pipeline"); err != nil {
					log.Printf("write response: %v", err)
				}
				return
			}
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			writer.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(writer).Encode(pipeline); err != nil {
				log.Printf("write response: %v", err)
			}

		}
	default:
		{
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}

func (a *App) pipelineIngestCSVHandler(writer http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	pipelineID, err := getIDFromURL(r, "pipeline_id")
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		if _, werr := fmt.Fprintf(writer, "Error during getting id: %v", err); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}
	runID := -1
	runID, err = getIDFromURL(r, "run_id")
	if err != nil {
		if err == errEMPTY {
			if runID, err = createRun(ctx, a.pool); err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				if _, werr := fmt.Fprintf(writer, "Error during creating new run: %v\n", err); werr != nil {
					log.Printf("write response: %v", werr)
				}
				return
			}
		} else {
			writer.WriteHeader(http.StatusBadRequest)
			if _, werr := fmt.Fprintf(writer, "Error during getting id: %v", err); werr != nil {
				log.Printf("write response: %v", werr)
			}
			return
		}
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

	pipeline := Pipeline{}
	if err := getPipelineByID(ctx, a.pool, pipelineID, &pipeline); err != nil {
		if err == pgx.ErrNoRows {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			writer.WriteHeader(http.StatusInternalServerError)
		}

		if _, err := fmt.Fprintf(writer, "Error during getting pipeline"); err != nil {
			log.Printf("write response: %v", err)
		}
		return
	}
	schema := Schema{}
	if err := json.Unmarshal(pipeline.Schema, &schema); err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		if _, werr := fmt.Fprintf(writer, "Error during getting fields"); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}

	reader := csv.NewReader(r.Body)
	header, err := reader.Read()
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		if _, werr := fmt.Fprintf(writer, "Error during reading csv header: %v", err); werr != nil {
			log.Printf("write response: %v", werr)
		}
		return
	}

	badRows, okRows, totalRows := 0, 0, 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			writer.WriteHeader(http.StatusInternalServerError)
			if _, werr := fmt.Fprintf(writer, "Error during reading csv: %v\n", err); werr != nil {
				log.Printf("write response: %v", werr)
			}
			return
		}
		totalRows++

		payload, vErr := validateRowRecord(schema, header, record)
		if vErr != nil {
			_, err = insertDeadLetter(ctx, a.pool, runID, strings.Join(record, ","), vErr.Error(), payload)
			if err != nil {
				log.Printf("Error during inserting Dead Letter with runID %d: %v\n", runID, err)
			}
			badRows++
			continue
		}

		if _, err := insertRow(ctx, a.pool, runID, payload); err != nil {
			_, err = insertDeadLetter(ctx, a.pool, runID, strings.Join(record, ","), err.Error(), payload)
			if err != nil {
				log.Printf("Error during inserting Dead Letter with runID %d: %v\n", runID, err)
			}
			badRows++
			continue
		}
		okRows++

	}
	if err := updateRunStats(ctx, a.pool, runID, totalRows, okRows, badRows); err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "Error during updating run: %v", err)
		return
	}

	res = RunEntry{}
	if err := getRunByID(ctx, a.pool, runID, &res); err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "Error during getting run: %v", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(res)

}
