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
