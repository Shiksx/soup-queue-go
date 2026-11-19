package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var schemaSequence int

// Run with a disposable database and a user that can create schemas. Each test
// gets an isolated schema; no existing jobs are read or truncated.
func fixture(t *testing.T) (Queue, context.Context) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is unset; needs disposable PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a postgres:// URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	t.Cleanup(cancel)
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	schemaSequence++
	name := fmt.Sprintf("test_queue_%d_%d", time.Now().UnixNano(), schemaSequence)
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanup, "DROP SCHEMA "+name+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	qp := u.Query()
	qp.Set("search_path", name)
	u.RawQuery = qp.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(24)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		t.Fatal(err)
	}
	return Queue{db}, ctx
}

func newJob(t *testing.T, ctx context.Context, q Queue, key string, fail bool) Job {
	t.Helper()
	j, created, err := q.create(ctx, key, Input{key, fail})
	if err != nil || !created {
		t.Fatalf("create %q: %+v, %v, %v", key, j, created, err)
	}
	return j
}

func claimOne(t *testing.T, ctx context.Context, q Queue, lease time.Duration) Job {
	t.Helper()
	j, err := q.claim(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func assertState(t *testing.T, ctx context.Context, q Queue, id int64, expected string, effects int) {
	t.Helper()
	j, err := q.get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var count, metadataMismatches int
	err = q.db.QueryRowContext(ctx, `
SELECT count(*), count(*) FILTER(WHERE e.attempt<>j.attempt OR e.value<>j.input)
FROM job_effects e JOIN jobs j ON e.job_id=j.id WHERE e.job_id=$1`, id).Scan(&count, &metadataMismatches)
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != expected || count != effects || metadataMismatches != 0 || (j.LeaseUntil != nil) != (expected == "running") {
		t.Fatalf("job %+v: wanted status %s, effects %d; got %d effects (%d bad metadata)", j, expected, effects, count, metadataMismatches)
	}
}

func TestClaimsAndIsolation(t *testing.T) {
	q, ctx := fixture(t)
	j := newJob(t, ctx, q, "row-locked-by-another-transaction", false)
	second := newJob(t, ctx, q, "available", false)
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var isolation string
	if err := tx.QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
		t.Fatal(err)
	}
	if isolation != "read committed" {
		t.Fatalf("test expects read committed; got %q", isolation)
	}
	var locked int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	j2 := claimOne(t, ctx, q, 2*time.Second)
	if j2.ID != second.ID {
		t.Fatalf("claimed %+v, wanted unlocked %d", j2, second.ID)
	}
	if _, err := q.claim(ctx, 2*time.Second); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("double claim or blocked: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	const claimants = 8
	type result struct {
		j   Job
		err error
	}
	out := make(chan result, claimants)
	start := make(chan struct{})
	for i := 0; i < claimants; i++ {
		go func() { <-start; got, e := q.claim(ctx, 8*time.Second); out <- result{got, e} }()
	}
	close(start)
	winners := 0
	for i := 0; i < claimants; i++ {
		r := <-out
		if r.err == nil {
			winners++
			if r.j.ID != j.ID || r.j.Attempt != 1 {
				t.Errorf("unexpected winner %+v", r.j)
			}
		} else if !errors.Is(r.err, sql.ErrNoRows) {
			t.Error(r.err)
		}
	}
	if winners != 1 {
		t.Fatalf("wanted 1 winner, got %d", winners)
	}
	t.Log("READ COMMITTED: skipped locked head; 1 winner among 8 simultaneous claimants")
}

func TestZombieAndClockSkew(t *testing.T) {
	q, ctx := fixture(t)
	newJob(t, ctx, q, "stale-worker", false)
	a := claimOne(t, ctx, q, 8*time.Second)
	// Force time to advance for this lease using SQL, not flaky sleeps.
	if _, err := q.db.ExecContext(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", a.ID); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(24 * time.Hour)
	a.LeaseUntil = &future
	if ok, err := q.finish(ctx, a); ok || err != nil {
		t.Fatalf("expired finish=%v,%v", ok, err)
	}
	b := claimOne(t, ctx, q, 8*time.Second)
	if b.ID != a.ID || b.Attempt != a.Attempt+1 {
		t.Fatalf("reclaim: before=%+v after=%+v", a, b)
	}
	if ok, err := q.finish(ctx, a); ok || err != nil {
		t.Fatalf("zombie finish while reclaimed=%v,%v", ok, err)
	}
	assertState(t, ctx, q, b.ID, "running", 0)
	past := time.Now().Add(-24 * time.Hour)
	b.LeaseUntil = &past
	if ok, err := q.finish(ctx, b); !ok || err != nil {
		t.Fatalf("winner finish=%v,%v", ok, err)
	}
	if ok, err := q.finish(ctx, a); ok || err != nil {
		t.Fatalf("zombie finish after winner=%v,%v", ok, err)
	}
	assertState(t, ctx, q, b.ID, "succeeded", 1)
	t.Log("expired lease rejected before and after reassignment; client-supplied +/-24h lease representations ignored; system/DB clock jumps not tested")
}

func TestCancelVersusComplete(t *testing.T) {
	q, ctx := fixture(t)
	queued := newJob(t, ctx, q, "queued-cancel", false)
	if _, err := q.cancel(ctx, queued.ID); err != nil {
		t.Fatal(err)
	}
	assertState(t, ctx, q, queued.ID, "cancelled", 0)
	newJob(t, ctx, q, "cancel-wins", false)
	cancels := claimOne(t, ctx, q, 5*time.Second)
	if _, err := q.cancel(ctx, cancels.ID); err != nil {
		t.Fatal(err)
	}
	if ok, err := q.finish(ctx, cancels); ok || err != nil {
		t.Fatalf("cancelled finish=%v,%v", ok, err)
	}
	assertState(t, ctx, q, cancels.ID, "cancelled", 0)

	newJob(t, ctx, q, "complete-wins", false)
	completed := claimOne(t, ctx, q, 5*time.Second)
	if ok, err := q.finish(ctx, completed); !ok || err != nil {
		t.Fatalf("first finish=%v,%v", ok, err)
	}
	if _, err := q.cancel(ctx, completed.ID); err != nil {
		t.Fatal(err)
	}
	assertState(t, ctx, q, completed.ID, "succeeded", 1)

	newJob(t, ctx, q, "race", false)
	race := claimOne(t, ctx, q, 5*time.Second)
	var finishOK bool
	var finishErr, cancelErr error
	var cancelJob Job
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() { defer wg.Done(); <-start; finishOK, finishErr = q.finish(ctx, race) }()
	go func() { defer wg.Done(); <-start; cancelJob, cancelErr = q.cancel(ctx, race.ID) }()
	close(start)
	wg.Wait()
	if finishErr != nil || cancelErr != nil {
		t.Fatalf("finish=%v cancel=%v", finishErr, cancelErr)
	}
	if finishOK {
		assertState(t, ctx, q, race.ID, "succeeded", 1)
		if cancelJob.Status != "succeeded" {
			t.Fatalf("cancel returned %+v", cancelJob)
		}
	} else {
		assertState(t, ctx, q, race.ID, "cancelled", 0)
		if cancelJob.Status != "cancelled" {
			t.Fatalf("cancel returned %+v", cancelJob)
		}
	}

	newJob(t, ctx, q, "fails", true)
	fail := claimOne(t, ctx, q, 5*time.Second)
	if ok, err := q.finish(ctx, fail); !ok || err != nil {
		t.Fatalf("terminal failure=%v,%v", ok, err)
	}
	assertState(t, ctx, q, fail.ID, "failed", 0)
	t.Logf("both orders verified; simultaneous cancel/complete winner: %s", cancelJob.Status)
}

func TestHTTPIdempotencyAndPagination(t *testing.T) {
	q, _ := fixture(t)
	s := httptest.NewServer(api(q))
	defer s.Close()
	type result struct {
		code int
		job  Job
		err  error
	}
	post := func(key, body string) result {
		req, err := http.NewRequest("POST", s.URL+"/jobs", strings.NewReader(body))
		if err != nil {
			return result{err: err}
		}
		req.Header.Set("Idempotency-Key", key)
		res, err := s.Client().Do(req)
		if err != nil {
			return result{err: err}
		}
		defer res.Body.Close()
		var j Job
		if res.StatusCode < 300 {
			err = json.NewDecoder(res.Body).Decode(&j)
		} else {
			_, err = io.Copy(io.Discard, res.Body)
		}
		return result{code: res.StatusCode, job: j, err: err}
	}
	const requests = 16
	responses := make(chan result, requests)
	for i := 0; i < requests; i++ {
		go func() { responses <- post("same-key", `{"input":"training A"}`) }()
	}
	var id int64
	created := 0
	for i := 0; i < requests; i++ {
		r := <-responses
		if r.err != nil || (r.code != 200 && r.code != 201) || r.job.ID < 1 {
			t.Fatalf("bad retry result: %+v", r)
		}
		if id == 0 {
			id = r.job.ID
		}
		if id != r.job.ID {
			t.Fatalf("retry returned %d instead of %d", r.job.ID, id)
		}
		if r.code == 201 {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("expected one insert, got %d", created)
	}
	if res := post("same-key", `{"input":"training B"}`); res.code != 409 || res.err != nil {
		t.Fatalf("expected 409, got %+v", res)
	}
	if res := post("bad-json", `{"input":"training C"} false`); res.code != 400 || res.err != nil {
		t.Fatalf("expected 400, got %+v", res)
	}
	j2 := post("unique", `{"input":"training A"}`)
	if j2.err != nil || j2.code != 201 {
		t.Fatalf("second insert: %+v", j2)
	}
	list := func(q string) (jobs []Job, next *int64) {
		res, err := s.Client().Get(s.URL + "/jobs?limit=1" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var page struct {
			Jobs []Job  `json:"jobs"`
			Next *int64 `json:"next_after"`
		}
		if err := json.NewDecoder(res.Body).Decode(&page); err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != 200 {
			t.Fatalf("list HTTP %d", res.StatusCode)
		}
		return page.Jobs, page.Next
	}
	jobs, next := list("")
	if len(jobs) != 1 || jobs[0].ID != id || next == nil || *next != id {
		t.Fatalf("wrong initial page %+v %v", jobs, next)
	}
	jobs, next = list(fmt.Sprintf("&after=%d", id))
	if len(jobs) != 1 || jobs[0].ID != j2.job.ID || next != nil {
		t.Fatalf("wrong last page %+v %v", jobs, next)
	}
	t.Log("16 concurrent POSTs: one 201, fifteen 200, same ID; mismatching payload 409; paginated exactly two jobs")
}

func TestLeaseExpiresWhileFinishWaitsForLock(t *testing.T) {
	q, ctx := fixture(t)
	newJob(t, ctx, q, "blocked-completion", false)
	j := claimOne(t, ctx, q, 8*time.Second)
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID); err != nil {
		t.Fatal(err)
	}
	type result struct {
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() { ok, e := q.finish(ctx, j); done <- result{ok, e} }()
	// Observe the actual blocked query, rather than assuming a sleep creates a race.
	for {
		var waiting bool
		err := q.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
		 WHERE pid<>pg_backend_pid() AND wait_event_type='Lock'
		 AND query='SELECT id FROM jobs WHERE id=$1 FOR UPDATE')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case got := <-done:
			t.Fatalf("finish did not block: %+v", got)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err = tx.ExecContext(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.ok || got.err != nil {
		t.Fatalf("expired blocked finish: %+v", got)
	}
	assertState(t, ctx, q, j.ID, "running", 0)
	reclaimed := claimOne(t, ctx, q, 8*time.Second)
	if ok, err := q.finish(ctx, reclaimed); !ok || err != nil {
		t.Fatalf("reclaim finish: %v %v", ok, err)
	}
	assertState(t, ctx, q, j.ID, "succeeded", 1)
	t.Log("observed finish blocked on row lock; lease expired before release; stale completion rejected")
}

func TestEffectAndTerminalStateAreAtomic(t *testing.T) {
	q, ctx := fixture(t)
	newJob(t, ctx, q, "effect-storage-error", false)
	j := claimOne(t, ctx, q, 8*time.Second)
	// Inject an effect-write failure after the jobs UPDATE would have run.
	if _, err := q.db.ExecContext(ctx, "ALTER TABLE job_effects ADD CONSTRAINT break_effects CHECK(false)"); err != nil {
		t.Fatal(err)
	}
	if ok, err := q.finish(ctx, j); ok || err == nil {
		t.Fatalf("effect write unexpectedly succeeded: %v %v", ok, err)
	}
	assertState(t, ctx, q, j.ID, "running", 0)
	if _, err := q.db.ExecContext(ctx, "ALTER TABLE job_effects DROP CONSTRAINT break_effects"); err != nil {
		t.Fatal(err)
	}
	if ok, err := q.finish(ctx, j); !ok || err != nil {
		t.Fatalf("healthy effect write: %v %v", ok, err)
	}
	assertState(t, ctx, q, j.ID, "succeeded", 1)
	t.Log("forced SQL error on effect insert rolled back terminal transition")
}
