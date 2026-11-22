package topn

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"infini.sh/framework/core/util"
)

const responseLimit = 24 << 20

var errResponseBudget = fmt.Errorf("response budget exhausted: %w", errBudget)

// budgetedCaller is used sequentially within a request, including route changes.
// Cleanup has its own small allowance so exhaustion cannot prevent PIT deletion.
func budgetedCaller(call caller, remaining int64) caller {
	return func(ctx context.Context, method, path string, body dsl) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		limit := remaining
		if method == http.MethodDelete {
			limit = 64 << 10
		}
		if limit <= 0 {
			return nil, errResponseBudget
		}
		raw, err := call(util.WithResponseBodyLimit(ctx, limit), method, path, body)
		if ctx.Err() != nil {
			return raw, ctx.Err()
		}
		if errors.Is(err, util.ErrResponseBodyTooLarge) || int64(len(raw)) > limit {
			if method != http.MethodDelete {
				remaining = 0
			}
			return nil, errResponseBudget
		}
		if method != http.MethodDelete {
			remaining -= int64(len(raw))
		}
		return raw, err
	}
}

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
