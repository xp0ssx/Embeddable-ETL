package main

import (
	"time"
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
