package main

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"time"

	_ "github.com/lib/pq"
)

//go:embed schema.sql
var schema string

var errKeyConflict = errors.New("idempotency key already used for a different request")

const fields = "id, input, fail, status, attempt, lease_until"
const claimedFields = "j.id, j.input, j.fail, j.status, j.attempt, j.lease_until"

type Input struct {
	Input string `json:"input"`
	Fail  bool   `json:"fail"`
}

type Job struct {
	ID int64 `json:"id"`
	Input
	Status     string     `json:"status"`
	Attempt    int64      `json:"attempt"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`
}

type scanner interface{ Scan(...any) error }

func scanJob(s scanner) (j Job, err error) {
	err = s.Scan(&j.ID, &j.Input.Input, &j.Fail, &j.Status, &j.Attempt, &j.LeaseUntil)
	return
}

type Queue struct{ db *sql.DB }

func (q Queue) create(ctx context.Context, key string, in Input) (Job, bool, error) {
	j, err := scanJob(q.db.QueryRowContext(ctx, "INSERT INTO jobs(idempotency_key,input,fail) VALUES($1,$2,$3) ON CONFLICT (idempotency_key) DO NOTHING RETURNING "+fields, key, in.Input, in.Fail))
	if !errors.Is(err, sql.ErrNoRows) {
		return j, err == nil, err
	}
	// A separate statement is intentional: a conflicting insert may have committed
	// after the INSERT's snapshot. Jobs are never deleted in this demo.
	j, err = scanJob(q.db.QueryRowContext(ctx, "SELECT "+fields+" FROM jobs WHERE idempotency_key=$1", key))
	if err == nil && j.Input != in {
		return j, false, errKeyConflict
	}
	return j, false, err
}

func (q Queue) get(ctx context.Context, id int64) (Job, error) {
	return scanJob(q.db.QueryRowContext(ctx, "SELECT "+fields+" FROM jobs WHERE id=$1", id))
}

func (q Queue) list(ctx context.Context, after int64, limit int) ([]Job, error) {
	rows, err := q.db.QueryContext(ctx, "SELECT "+fields+" FROM jobs WHERE id>$1 ORDER BY id LIMIT $2", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0, limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (q Queue) cancel(ctx context.Context, id int64) (Job, error) {
	j, err := scanJob(q.db.QueryRowContext(ctx, "UPDATE jobs SET status='cancelled', lease_until=NULL WHERE id=$1 AND status IN ('queued','running') RETURNING "+fields, id))
	if errors.Is(err, sql.ErrNoRows) {
		return q.get(ctx, id)
	}
	return j, err
}

func (q Queue) claim(ctx context.Context, lease time.Duration) (Job, error) {
	// statement_timestamp is stable for the eligibility scan; the deadline starts
	// from the DB clock at the actual row update, not the worker's wall clock.
	return scanJob(q.db.QueryRowContext(ctx, `
WITH candidate AS (
    SELECT id FROM jobs
    WHERE status='queued' OR (status='running' AND lease_until <= statement_timestamp())
    ORDER BY id
    FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE jobs j
SET status='running', attempt=j.attempt+1,
    lease_until=clock_timestamp()+($1::bigint * interval '1 millisecond')
FROM candidate c WHERE j.id=c.id
RETURNING `+claimedFields, lease.Milliseconds()))
}

func (q Queue) finish(ctx context.Context, j Job) (bool, error) {
	tx, err := q.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// Lock first. A validity condition evaluated before waiting on a row lock
	// could accept a lease that expired while waiting.
	var lockedID int64
	err = tx.QueryRowContext(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID).Scan(&lockedID)
	if err != nil {
		return false, err
	}

	var fail bool
	var output string
	err = tx.QueryRowContext(ctx, `
UPDATE jobs SET status=CASE WHEN fail THEN 'failed' ELSE 'succeeded' END,
    lease_until=NULL
WHERE id=$1 AND status='running' AND attempt=$2 AND lease_until > clock_timestamp()
RETURNING fail, input`, j.ID, j.Attempt).Scan(&fail, &output)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !fail {
		// Deliberately no ON CONFLICT: a duplicate attempt to write a durable
		// effect should roll back and surface a bug, not vanish as a success.
		_, err = tx.ExecContext(ctx,
			"INSERT INTO job_effects(job_id,attempt,value) VALUES($1,$2,$3)",
			j.ID, j.Attempt, output)
		if err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
