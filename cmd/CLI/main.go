package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	baseURL := getenv("ETL_URL", "http://localhost:8080")

	switch os.Args[1] {
	case "health":
		callGET(baseURL + "/dev/healthz")
	case "ready":
		callGET(baseURL + "/dev/readyz")

	case "run-create":
		callPOSTJSON(baseURL+"/dev/runs", nil)

	case "run-get":
		fs := flag.NewFlagSet("run-get", flag.ExitOnError)
		id := fs.Int("id", 0, "run id")
		fs.Parse(os.Args[2:])
		if *id <= 0 {
			fatal("id required")
		}
		callGET(fmt.Sprintf("%s/dev/runs?id=%d", baseURL, *id))

	case "row-insert":
		fs := flag.NewFlagSet("row-insert", flag.ExitOnError)
		runID := fs.Int("run-id", 0, "run id")
		jsonPath := fs.String("json", "", "path to json payload")
		fs.Parse(os.Args[2:])
		if *runID <= 0 || *jsonPath == "" {
			fatal("run-id and json are required")
		}
		body := mustRead(*jsonPath)
		callPOSTJSON(fmt.Sprintf("%s/dev/rows?run_id=%d", baseURL, *runID), body)

	case "ingest-csv":
		fs := flag.NewFlagSet("ingest-csv", flag.ExitOnError)
		runID := fs.Int("run-id", 0, "optional run id")
		csvPath := fs.String("csv", "", "path to csv file")
		fs.Parse(os.Args[2:])
		if *csvPath == "" {
			fatal("csv is required")
		}
		url := fmt.Sprintf("%s/v1/ingest/csv", baseURL)
		if *runID > 0 {
			url = fmt.Sprintf("%s?run_id=%d", url, *runID)
		}
		body := mustRead(*csvPath)
		callPOSTCSV(url, body)

	case "pipeline-create":
		fs := flag.NewFlagSet("pipeline-create", flag.ExitOnError)
		name := fs.String("name", "", "pipeline name")
		schemaPath := fs.String("schema", "", "path to schema json")
		fs.Parse(os.Args[2:])
		if *schemaPath == "" {
			fatal("schema is required")
		}
		schemaBytes := mustRead(*schemaPath)
		req := map[string]any{
			"name":   *name,
			"schema": json.RawMessage(schemaBytes),
		}
		body, _ := json.Marshal(req)
		callPOSTJSON(baseURL+"/v1/pipelines", body)

	case "pipeline-get":
		fs := flag.NewFlagSet("pipeline-get", flag.ExitOnError)
		id := fs.Int("id", 0, "pipeline id")
		fs.Parse(os.Args[2:])
		if *id <= 0 {
			fatal("id required")
		}
		callGET(fmt.Sprintf("%s/v1/pipelines?pipeline_id=%d", baseURL, *id))

	case "pipeline-ingest-csv":
		fs := flag.NewFlagSet("pipeline-ingest-csv", flag.ExitOnError)
		pipelineID := fs.Int("pipeline-id", 0, "pipeline id")
		runID := fs.Int("run-id", 0, "optional run id")
		csvPath := fs.String("csv", "", "path to csv file")
		fs.Parse(os.Args[2:])
		if *pipelineID <= 0 || *csvPath == "" {
			fatal("pipeline-id and csv are required")
		}
		url := fmt.Sprintf("%s/v1/pipelines/ingest/csv?pipeline_id=%d", baseURL, *pipelineID)
		if *runID > 0 {
			url += fmt.Sprintf("&run_id=%d", *runID)
		}
		body := mustRead(*csvPath)
		callPOSTCSV(url, body)

	default:
		usage()
	}
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  health")
	fmt.Println("  ready")
	fmt.Println("  run-create")
	fmt.Println("  run-get -id <run_id>")
	fmt.Println("  row-insert -run-id <run_id> -json <file>")
	fmt.Println("  ingest-csv -csv <file> [-run-id <run_id>]")
	fmt.Println("  pipeline-create -name <name> -schema <file>")
	fmt.Println("  pipeline-get -id <pipeline_id>")
	fmt.Println("  pipeline-ingest-csv -pipeline-id <id> -csv <file> [-run-id <run_id>]")
	fmt.Println("Env:")
	fmt.Println("  ETL_URL=http://localhost:8080")
}

func callGET(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fatal(err.Error())
	}
	defer resp.Body.Close()
	printResp(resp)
}

func callPOSTJSON(url string, body []byte) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	resp, err := http.Post(url, "application/json", r)
	if err != nil {
		fatal(err.Error())
	}
	defer resp.Body.Close()
	printResp(resp)
}

func callPOSTCSV(url string, body []byte) {
	resp, err := http.Post(url, "text/csv", bytes.NewReader(body))
	if err != nil {
		fatal(err.Error())
	}
	defer resp.Body.Close()
	printResp(resp)
}

func printResp(resp *http.Response) {
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("status: %d\n", resp.StatusCode)
	if len(out) > 0 {
		fmt.Println(strings.TrimSpace(string(out)))
	}
}

func mustRead(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		fatal(err.Error())
	}
	return b
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
