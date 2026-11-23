package topn

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBackendTimeoutKeepsRotatedPIT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := snapshot{id: "before", shards: 3, call: func(_ context.Context, _, _ string, body dsl) ([]byte, error) {
		if timeout, ok := body["timeout"].(string); !ok || !strings.HasSuffix(timeout, "ms") {
			t.Fatalf("missing backend timeout: %v", body)
		}
		return []byte(`{"pit_id":"after",` + allShards + `,"timed_out":true}`), nil
	}}
	var out interface{}
	err := s.search(ctx, dsl{}, dsl{}, &out)
	if failureStatus(err) != "timed_out" || s.id != "after" || out != nil {
		t.Fatalf("err=%v id=%s out=%v", err, s.id, out)
	}
}

func TestPITCloseAcknowledgements(t *testing.T) {
	for _, tc := range []struct {
		wire         string
		scalar, want bool
	}{
		{`{"pits":[{"pit_id":"current","successful":true}]}`, false, true},
		{`{"pits":[{"pit_id":"current","successful":false}]}`, false, false},
		{`{"pits":[{"pit_id":"old","successful":true}]}`, false, false},
		{`{"succeeded":true}`, true, true}, {`{"succeeded":false}`, true, false},
	} {
		if ok := checkPITClose([]byte(tc.wire), "current", tc.scalar) == nil; ok != tc.want {
			t.Error(tc)
		}
	}
}

func TestShorterTimeout(t *testing.T) {
	for _, tc := range []struct {
		arg  string
		want time.Duration
	}{{"", 5 * time.Second}, {"60s", 5 * time.Second}, {"2s", 2 * time.Second}} {
		if got, err := queryTimeout(tc.arg); err != nil || got != tc.want {
			t.Fatalf("%q: %s %v", tc.arg, got, err)
		}
	}
	if _, err := queryTimeout("0s"); err == nil {
		t.Fatal("zero timeout accepted")
	}
}
