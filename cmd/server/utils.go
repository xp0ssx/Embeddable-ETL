package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func createRun(ctx context.Context, pool *pgxpool.Pool) (runID int, err error) {
	row := pool.QueryRow(ctx, `
		INSERT INTO runs DEFAULT VALUES
		RETURNING id;`)
	if err := row.Scan(&runID); err != nil {
		return -1, err
	}
	return runID, nil
}

func getRunByID(ctx context.Context, pool *pgxpool.Pool, runID int, res *RunEntry) error {
	row := pool.QueryRow(ctx, `
		SELECT id, created_at, finished_at, status, total_rows, ok_rows, bad_rows FROM runs
		WHERE id = ($1)`, runID)
	if err := row.Scan(&res.Id, &res.Created_at, &res.Finished_at, &res.Status, &res.Total_rows, &res.Ok_rows, &res.Bad_rows); err != nil {
		return err
	}
	return nil
}

func updateRunStats(ctx context.Context, pool *pgxpool.Pool, runID, total, ok, bad int) error {
	status := "completed"
	if bad > 0 {
		status = "completed_with_errors"
	}
	_, err := pool.Exec(ctx, `
		UPDATE runs
		SET total_rows = $1,
			ok_rows = $2,
			bad_rows = $3,
			finished_at = now(),
			status = $4
		WHERE id = $5;`, total, ok, bad, status, runID)
	if err != nil {
		updateErr := err
		if err := markRunFailed(ctx, pool, runID); err != nil {
			log.Printf("Error during marking run id %d: %v\n", runID, err)
		}
		return updateErr
	}
	return nil
}

func markRunFailed(ctx context.Context, pool *pgxpool.Pool, runID int) error {
	var err error
	for i := 1; i <= 3; i++ {
		_, err = pool.Exec(ctx, `
		UPDATE runs
		SET status = 'failed',
			finished_at = now()
		WHERE id = $1;`, runID)
		if err == nil {
			return nil
		}
	}
	return err
}
func insertRow(ctx context.Context, pool *pgxpool.Pool, runID int, payload map[string]any) (rowID int, err error) {
	row := pool.QueryRow(ctx, `
		INSERT INTO etl_rows(run_id, payload)
		VALUES ($1, $2)
		RETURNING id;`, runID, payload)
	if err := row.Scan(&rowID); err != nil {
		return -1, err
	}
	return rowID, nil
}

func insertDeadLetter(ctx context.Context, pool *pgxpool.Pool, runID int, rawLine, errMsg string, payload map[string]any) (deadID int, err error) {
	row := pool.QueryRow(ctx, `
		INSERT INTO etl_dead_letters(run_id, raw_line, error, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id;`, runID, rawLine, errMsg, payload)
	if err := row.Scan(&deadID); err != nil {
		return -1, err
	}
	return deadID, nil
}

func insertPipeline(ctx context.Context, pool *pgxpool.Pool, name string, schema json.RawMessage) (pipelineID int, err error) {
	row := pool.QueryRow(ctx, `
	INSERT INTO pipelines(name, schema)
	VALUES ($1, $2)
	RETURNING id;`, name, schema)
	if err := row.Scan(&pipelineID); err != nil {
		return -1, err
	}
	return pipelineID, nil
}

func getPipelineByID(ctx context.Context, pool *pgxpool.Pool, pipelineID int, pipeline *Pipeline) error {
	row := pool.QueryRow(ctx, `
	SELECT id, name, schema, created_at FROM pipelines
	WHERE id=$1;`, pipelineID)
	if err := row.Scan(&pipeline.ID, &pipeline.Name, &pipeline.Schema, &pipeline.CreatedAt); err != nil {
		return err
	}
	return nil
}

var errEMPTY error = fmt.Errorf("empty")

func getIDFromURL(r *http.Request, name string) (id int, err error) {
	idStr := r.URL.Query().Get(name)
	if idStr == "" {
		return -1, errEMPTY
	}
	if id, err = strconv.Atoi(idStr); err != nil {
		return -1, err
	} else {
		return id, nil
	}
}

func validateRowRecord(schema Schema, header []string, record []string) (map[string]any, error) {
	if len(record) != len(header) {
		return nil, fmt.Errorf("wrong number of columns")
	}
	index := map[string]int{}
	for id, colName := range header {
		index[colName] = id
	}
	payload := map[string]any{}
	for _, field := range schema.Fields {
		idx, ok := index[field.Name]
		if !ok {
			if field.Required {
				return nil, fmt.Errorf("missing required field %s", field.Name)
			}
			continue
		}
		if len(record) > len(schema.Fields) {
			return nil, fmt.Errorf("too much columns")
		}
		rawVal := record[idx]
		if strings.TrimSpace(rawVal) == "" {
			if field.Required {
				return nil, fmt.Errorf("required field %s is empty", field.Name)
			}
			continue
		}
		if _, err := parseValue(rawVal, field); err != nil {
			return nil, err
		}
		if field.Format != "" {
			if tester, ok := formatmap[field.Format]; !ok {
				return nil, fmt.Errorf("unknown format: %s", field.Format)
			} else {
				if !tester(rawVal) {
					return nil, fmt.Errorf("value %s is not approved by format %s", rawVal, field.Format)
				}
			}
		}
		payload[field.Name] = rawVal
	}
	return payload, nil
}
