package main

import (
	"context"
	"log"

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
