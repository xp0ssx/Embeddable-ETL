package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPipelineIngestCSV_E2E(t *testing.T) {
	dsn := os.Getenv("ETL_DB_DSN")
	if dsn == "" {
		t.Skip("ETL_DB_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	migrationsPath := filepath.Join(wd, "..", "..", "migrations")
	if err := applyMigrations(ctx, pool, migrationsPath); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	app := &App{pool: pool}

	// 1) Create pipeline
	schema := Schema{Fields: []Field{
		{Name: "email", Type: "string", Required: true, Format: "email"},
		{Name: "age", Type: "int", Required: true},
		{Name: "note", Type: "string", Required: false},
	}}
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}

	pipelineReq, err := json.Marshal(PipelineRequest{
		Name:   "users_e2e",
		Schema: schemaBytes,
	})
	if err != nil {
		t.Fatalf("marshal pipeline request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/pipelines", bytes.NewReader(pipelineReq))
	rec := httptest.NewRecorder()
	app.pipelinesHandler(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var pipelineResp struct {
		PipelineID int `json:"pipeline_id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&pipelineResp); err != nil {
		t.Fatalf("decode pipeline response: %v", err)
	}
	if pipelineResp.PipelineID == 0 {
		t.Fatalf("expected pipeline id")
	}

	// 2) Ingest CSV using pipeline_id
	csvBody := strings.NewReader("email,age,note\nuser@example.com,30,ok\nnot-an-email,25,bad\n")
	ingestReq := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/v1/pipelines/ingest/csv?pipeline_id=%d", pipelineResp.PipelineID),
		csvBody,
	)
	ingestRec := httptest.NewRecorder()
	app.pipelineIngestCSVHandler(ingestRec, ingestReq)

	if ingestRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", ingestRec.Code, ingestRec.Body.String())
	}

	var runResp RunEntry
	if err := json.NewDecoder(ingestRec.Body).Decode(&runResp); err != nil {
		t.Fatalf("decode run response: %v", err)
	}

	// 3) Assert run stats
	if runResp.Total_rows != 2 || runResp.Ok_rows != 1 || runResp.Bad_rows != 1 {
		t.Fatalf("unexpected run stats: total=%d ok=%d bad=%d", runResp.Total_rows, runResp.Ok_rows, runResp.Bad_rows)
	}

	// 4) Assert DB rows using run_id (no global checks)
	var rowCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM etl_rows WHERE run_id=$1", runResp.Id).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected 1 row, got %d", rowCount)
	}

	var deadCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM etl_dead_letters WHERE run_id=$1", runResp.Id).Scan(&deadCount); err != nil {
		t.Fatalf("count dead letters: %v", err)
	}
	if deadCount != 1 {
		t.Fatalf("expected 1 dead letter, got %d", deadCount)
	}
}

func TestPipelineIngestCSV_EmptyRequired(t *testing.T) {
	dsn := os.Getenv("ETL_DB_DSN")
	if dsn == "" {
		t.Skip("ETL_DB_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	wd, _ := os.Getwd()
	migrationsPath := filepath.Join(wd, "..", "..", "migrations")
	if err := applyMigrations(ctx, pool, migrationsPath); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	app := &App{pool: pool}

	schema := Schema{Fields: []Field{
		{Name: "email", Type: "string", Required: true, Format: "email"},
		{Name: "age", Type: "int", Required: true},
	}}
	schemaBytes, _ := json.Marshal(schema)

	reqBody, _ := json.Marshal(PipelineRequest{Name: "req_empty", Schema: schemaBytes})
	req := httptest.NewRequest(http.MethodPost, "/v1/pipelines", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	app.pipelinesHandler(rec, req)

	var pResp struct {
		PipelineID int `json:"pipeline_id"`
	}
	json.NewDecoder(rec.Body).Decode(&pResp)

	csvBody := strings.NewReader("email,age\n,22\n")
	ingestReq := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/v1/pipelines/ingest/csv?pipeline_id=%d", pResp.PipelineID),
		csvBody,
	)
	ingestRec := httptest.NewRecorder()
	app.pipelineIngestCSVHandler(ingestRec, ingestReq)

	var runResp RunEntry
	json.NewDecoder(ingestRec.Body).Decode(&runResp)

	if runResp.Total_rows != 1 || runResp.Ok_rows != 0 || runResp.Bad_rows != 1 {
		t.Fatalf("unexpected run stats: total=%d ok=%d bad=%d", runResp.Total_rows, runResp.Ok_rows, runResp.Bad_rows)
	}
}

func TestPipelineIngestCSV_InvalidType(t *testing.T) {
	dsn := os.Getenv("ETL_DB_DSN")
	if dsn == "" {
		t.Skip("ETL_DB_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	wd, _ := os.Getwd()
	migrationsPath := filepath.Join(wd, "..", "..", "migrations")
	if err := applyMigrations(ctx, pool, migrationsPath); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	app := &App{pool: pool}

	schema := Schema{Fields: []Field{
		{Name: "email", Type: "string", Required: true, Format: "email"},
		{Name: "age", Type: "int", Required: true},
	}}
	schemaBytes, _ := json.Marshal(schema)

	reqBody, _ := json.Marshal(PipelineRequest{Name: "bad_type", Schema: schemaBytes})
	req := httptest.NewRequest(http.MethodPost, "/v1/pipelines", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	app.pipelinesHandler(rec, req)

	var pResp struct {
		PipelineID int `json:"pipeline_id"`
	}
	json.NewDecoder(rec.Body).Decode(&pResp)

	csvBody := strings.NewReader("email,age\nuser@example.com,abc\n")
	ingestReq := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/v1/pipelines/ingest/csv?pipeline_id=%d", pResp.PipelineID),
		csvBody,
	)
	ingestRec := httptest.NewRecorder()
	app.pipelineIngestCSVHandler(ingestRec, ingestReq)

	var runResp RunEntry
	json.NewDecoder(ingestRec.Body).Decode(&runResp)

	if runResp.Total_rows != 1 || runResp.Ok_rows != 0 || runResp.Bad_rows != 1 {
		t.Fatalf("unexpected run stats: total=%d ok=%d bad=%d", runResp.Total_rows, runResp.Ok_rows, runResp.Bad_rows)
	}
}

func TestPipelineIngestCSV_InvalidEmail(t *testing.T) {
	dsn := os.Getenv("ETL_DB_DSN")
	if dsn == "" {
		t.Skip("ETL_DB_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	wd, _ := os.Getwd()
	migrationsPath := filepath.Join(wd, "..", "..", "migrations")
	if err := applyMigrations(ctx, pool, migrationsPath); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	app := &App{pool: pool}

	schema := Schema{Fields: []Field{
		{Name: "email", Type: "string", Required: true, Format: "email"},
		{Name: "age", Type: "int", Required: true},
	}}
	schemaBytes, _ := json.Marshal(schema)

	reqBody, _ := json.Marshal(PipelineRequest{Name: "bad_email", Schema: schemaBytes})
	req := httptest.NewRequest(http.MethodPost, "/v1/pipelines", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	app.pipelinesHandler(rec, req)

	var pResp struct {
		PipelineID int `json:"pipeline_id"`
	}
	json.NewDecoder(rec.Body).Decode(&pResp)

	csvBody := strings.NewReader("email,age\nnot-an-email,20\n")
	ingestReq := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/v1/pipelines/ingest/csv?pipeline_id=%d", pResp.PipelineID),
		csvBody,
	)
	ingestRec := httptest.NewRecorder()
	app.pipelineIngestCSVHandler(ingestRec, ingestReq)

	var runResp RunEntry
	json.NewDecoder(ingestRec.Body).Decode(&runResp)

	if runResp.Total_rows != 1 || runResp.Ok_rows != 0 || runResp.Bad_rows != 1 {
		t.Fatalf("unexpected run stats: total=%d ok=%d bad=%d", runResp.Total_rows, runResp.Ok_rows, runResp.Bad_rows)
	}
}
