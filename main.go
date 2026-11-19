package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func env(key, def string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return def
}

func durationMS(key, def string, min, max int64) (time.Duration, error) {
	n, err := strconv.ParseInt(env(key, def), 10, 64)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be %d–%d milliseconds", key, min, max)
	}
	return time.Duration(n) * time.Millisecond, nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func work(ctx context.Context, q Queue) error {
	cost, err := durationMS("WORK_MS", "40", 0, 600000)
	if err != nil {
		return err
	}
	lease, err := durationMS("LEASE_MS", "2000", 1, 600000)
	if err != nil {
		return err
	}
	if lease <= cost {
		return errors.New("LEASE_MS must be greater than WORK_MS")
	}
	for ctx.Err() == nil {
		// Bound database calls independently of the simulated execution.
		claimCtx, done := context.WithTimeout(ctx, 3*time.Second)
		j, err := q.claim(claimCtx, lease)
		done()
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) && ctx.Err() == nil {
				log.Printf("claim: %v", err)
			}
			sleep(ctx, 25*time.Millisecond)
			continue
		}
		if !sleep(ctx, cost) {
			break
		}
		finishCtx, done := context.WithTimeout(ctx, 3*time.Second)
		ok, err := q.finish(finishCtx, j)
		done()
		if err != nil && ctx.Err() == nil {
			log.Printf("finish job %d attempt %d: %v", j.ID, j.Attempt, err)
		}
		if err == nil && !ok {
			log.Printf("completion fenced: job %d attempt %d", j.ID, j.Attempt)
		}
	}
	return nil
}

func serve(ctx context.Context, q Queue) error {
	srv := &http.Server{
		Addr: env("HTTP_ADDR", ":8080"), Handler: api(q),
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 45 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()
	log.Printf("API listening on %s", srv.Addr)
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: queue migrate|api|worker")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("set DATABASE_URL")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(8)
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(startup); err != nil {
		return err
	}
	q := Queue{db}
	switch os.Args[1] {
	case "migrate":
		// One transaction even if the migration contains multiple statements.
		tx, err := db.BeginTx(startup, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(startup, schema); err != nil {
			return err
		}
		return tx.Commit()
	case "api":
		return serve(ctx, q)
	case "worker":
		return work(ctx, q)
	default:
		return errors.New("usage: queue migrate|api|worker")
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
