package main

import (
	"encoding/json"
	"fmt"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type RunEntry struct {
	Id          int        `json:"id"`
	Created_at  time.Time  `json:"created_at"`
	Finished_at *time.Time `json:"finished_at"`
	Status      string     `json:"status"`
	Total_rows  int        `json:"total_rows"`
	Ok_rows     int        `json:"ok_rows"`
	Bad_rows    int        `json:"bad_rows"`
}

type Pipeline struct {
	ID        int             `json:"id"`
	Name      string          `json:"name"`
	Schema    json.RawMessage `json:"schema"`
	CreatedAt time.Time       `json:"created_at"`
}

type PipelineRequest struct {
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Format   string `json:"format"`
}

var formatmap = map[string]func(string) bool{
	"email": func(s string) bool {
		_, err := mail.ParseAddress(s)
		return err == nil
	},
	"date": func(s string) bool {
		_, err := time.Parse("2006-01-02", s)
		return err == nil
	},
	"uuid": func(s string) bool {
		_, err := uuid.Parse(s)
		return err == nil
	},
}

var typeParsers = map[string]func(string) (any, error){
	"string": func(s string) (any, error) { return s, nil },
	"int":    func(s string) (any, error) { return strconv.Atoi(s) },
	"float":  func(s string) (any, error) { return strconv.ParseFloat(s, 64) },
	"bool":   func(s string) (any, error) { return strconv.ParseBool(s) },
}

func parseValue(raw string, f Field) (any, error) {
	parser, ok := typeParsers[strings.ToLower(f.Type)]
	if !ok {
		return nil, fmt.Errorf("unknown type %s", f.Type)
	}
	val, err := parser(raw)
	if err != nil {
		return nil, err
	}
	return val, nil
}
