package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"
)

func jsonResponse(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("encode: %v", err)
	}
}

func problem(w http.ResponseWriter, code int, message string) {
	jsonResponse(w, code, map[string]string{"error": message})
}

func queueError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		problem(w, http.StatusNotFound, "job not found")
	case errors.Is(err, errKeyConflict):
		problem(w, http.StatusConflict, errKeyConflict.Error())
	default:
		log.Printf("queue: %v", err)
		problem(w, http.StatusInternalServerError, "queue unavailable")
	}
}

func api(q Queue) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := q.db.PingContext(r.Context()); err != nil {
			problem(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		jsonResponse(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 {
			problem(w, http.StatusBadRequest, "supply one Idempotency-Key header")
			return
		}
		key := strings.TrimSpace(keys[0])
		if key == "" || len(key) > 128 || !utf8.ValidString(key) {
			problem(w, http.StatusBadRequest, "Idempotency-Key must be 1–128 UTF-8 bytes")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		var in *Input
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		invalid := dec.Decode(&in) != nil || in == nil ||
			strings.TrimSpace(in.Input) == "" || len(in.Input) > 1024 ||
			strings.ContainsRune(in.Input, 0) || dec.Decode(new(any)) != io.EOF
		if invalid {
			problem(w, http.StatusBadRequest, "expected a JSON object with nonempty input (up to 1024 bytes) and optional fail")
			return
		}
		j, created, err := q.create(r.Context(), key, *in)
		if err != nil {
			queueError(w, err)
			return
		}
		code := http.StatusOK
		if created {
			code = http.StatusCreated
		}
		w.Header().Set("Location", "/jobs/"+strconv.FormatInt(j.ID, 10))
		jsonResponse(w, code, j)
	})
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		const defaultLimit = 50
		after, limit := int64(0), defaultLimit
		if s := r.URL.Query().Get("after"); s != "" {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil || n < 0 {
				problem(w, 400, "invalid after cursor")
				return
			}
			after = n
		}
		if s := r.URL.Query().Get("limit"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 1 || n > 100 {
				problem(w, 400, "limit must be 1–100")
				return
			}
			limit = n
		}
		jobs, err := q.list(r.Context(), after, limit+1)
		if err != nil {
			queueError(w, err)
			return
		}
		var next *int64
		if len(jobs) > limit {
			jobs = jobs[:limit]
			next = &jobs[limit-1].ID
		}
		jsonResponse(w, 200, struct {
			Jobs []Job  `json:"jobs"`
			Next *int64 `json:"next_after"`
		}{jobs, next})
	})
	byID := func(op func(*http.Request, int64) (Job, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
			if err != nil || id < 1 {
				problem(w, 400, "invalid job id")
				return
			}
			j, err := op(r, id)
			if err != nil {
				queueError(w, err)
				return
			}
			jsonResponse(w, 200, j)
		}
	}
	mux.HandleFunc("GET /jobs/{id}", byID(func(r *http.Request, id int64) (Job, error) { return q.get(r.Context(), id) }))
	mux.HandleFunc("POST /jobs/{id}/cancel", byID(func(r *http.Request, id int64) (Job, error) { return q.cancel(r.Context(), id) }))
	return mux
}
