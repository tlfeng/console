package topn

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func searchTimeout(ctx context.Context, body dsl) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline).Milliseconds()
		if remaining < 1 {
			return context.DeadlineExceeded
		}
		body["timeout"] = fmt.Sprintf("%dms", remaining)
	}
	return nil
}

func queryTimeout(raw string) (time.Duration, error) {
	limit := 5 * time.Second
	if raw == "" {
		return limit, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, errors.New("invalid TopN timeout")
	}
	return min(limit, d), nil
}

// summaryWaitTimeout bounds how long one request waits for an on-demand
// summary job before reporting busy; a later request reuses the retained job.
const summaryWaitTimeout = 30 * time.Second

func queryWaitTimeout(raw string) time.Duration {
	if raw == "" {
		return summaryWaitTimeout
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return min(summaryWaitTimeout, d)
	}
	return summaryWaitTimeout
}
