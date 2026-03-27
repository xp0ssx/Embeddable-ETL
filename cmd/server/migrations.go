package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func applyOneMigration(ctx context.Context, pool *pgxpool.Pool, version, sqlText string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error during creating transaction on migration %s: %w", version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, sqlText); err != nil {
		return fmt.Errorf("Error during applying migration %s: %w", version, err)
	}

	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version); err != nil {
		return fmt.Errorf("Error during adding info about migration %s: %w", version, err)
	}

	return tx.Commit(ctx)
}

func applyMigrations(ctx context.Context, pool *pgxpool.Pool, pathMigDir string) error {
	_, err := pool.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS schema_migrations (
    version text primary key,
    applied_at timestamptz NOT NULL default now()
	);
`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("SQL error: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)

	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return fmt.Errorf("Error during reading: %w", err)
		}
		applied[val] = true
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error with sql query: %w", err)
	}

	migDirs, err := os.ReadDir(pathMigDir)
	if err != nil {
		return fmt.Errorf("Error during reading work directory: %w", err)
	}

	versions := make([]string, 0, len(migDirs))
	for id := range migDirs {
		if !migDirs[id].IsDir() {
			if strings.HasSuffix(migDirs[id].Name(), ".sql") {
				versions = append(versions, migDirs[id].Name())
			}
		}
	}
	slices.Sort(versions)

	for _, entry := range versions {
		if !applied[entry] {
			if sqlBytes, err := os.ReadFile(filepath.Join(pathMigDir, entry)); err != nil {
				return fmt.Errorf("Error during reading migration file %s: %w", entry, err)
			} else {
				if err := applyOneMigration(ctx, pool, entry, string(sqlBytes)); err != nil {
					return fmt.Errorf("Error during applying transaction %s: %w", entry, err)
				}
				log.Printf("applied migration %s", entry)
			}
		}
	}
	return nil
}
