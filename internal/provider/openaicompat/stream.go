package openaicompat

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
)

const (
	maxStreamAggregateBytes = 2 << 20
	maxStreamTotalBytes     = 10 << 20
	maxSSEDataFields        = 4096
	maxSSEEvents            = 4096
	maxToolIndexes          = 128
	maxToolFragments        = 2048
)

func (c *Client) Stream(ctx context.Context, input domain.ModelRequest) (<-chan domain.ModelEvent, error) {
	resp, retriesUsed, err := c.openWithRetries(ctx, input)
	if err != nil {
		return nil, err
	}
	out := make(chan domain.ModelEvent)
	go func() {
		defer close(out)
		for {
			observed, err := c.consumeAttempt(ctx, resp, out)
			if err == nil || ctx.Err() != nil {
				return
			}
			if observed || !IsRetryable(err) || retriesUsed >= len(c.retryDelays) {
				sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelStreamError, Err: err})
				return
			}
			if err := c.waitForRetry(ctx, c.retryDelays[retriesUsed]); err != nil {
				return
			}
			retriesUsed++
			for {
				resp, err = c.openAttempt(ctx, input)
				if err == nil {
					break
				}
				if !IsRetryable(err) || retriesUsed >= len(c.retryDelays) {
					sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelStreamError, Err: err})
					return
				}
				if err := c.waitForRetry(ctx, c.retryDelays[retriesUsed]); err != nil {
					return
				}
				retriesUsed++
			}
		}
	}()
	return out, nil
}

func (c *Client) openWithRetries(ctx context.Context, input domain.ModelRequest) (*http.Response, int, error) {
	retriesUsed := 0
	for {
		resp, err := c.openAttempt(ctx, input)
		if err == nil {
			return resp, retriesUsed, nil
		}
		if !IsRetryable(err) || retriesUsed >= len(c.retryDelays) {
			return nil, retriesUsed, err
		}
		if err := c.waitForRetry(ctx, c.retryDelays[retriesUsed]); err != nil {
			return nil, retriesUsed, err
		}
		retriesUsed++
	}
}

func (c *Client) waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(c.jitter(delay))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type callParts struct {
	id        strings.Builder
	name      strings.Builder
	arguments strings.Builder
}

func (c *Client) consumeAttempt(ctx context.Context, resp *http.Response, out chan<- domain.ModelEvent) (bool, error) {
	defer resp.Body.Close()

	observed := false
	calls := map[int]*callParts{}
	toolBytes := 0
	toolFragments := 0
	dataFields := 0
	eventBytes := 0
	eventCount := 0
	streamBytes := 0
	var data strings.Builder
	dispatch := func() (bool, error) {
		if dataFields == 0 {
			return false, nil
		}
		eventCount++
		if eventCount > maxSSEEvents {
			return false, fatalStreamError("provider SSE event limit exceeded", nil)
		}
		encoded := data.String()
		data.Reset()
		dataFields = 0
		eventBytes = 0
		if encoded == "[DONE]" {
			if err := emitCompletedCalls(ctx, out, calls); err != nil {
				return true, err
			}
			if !sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelDone}) {
				return true, ctx.Err()
			}
			return true, nil
		}

		var chunk streamChunk
		if err := json.Unmarshal([]byte(encoded), &chunk); err != nil {
			if observed {
				return false, interruptedError(err)
			}
			return false, fatalStreamError("decode provider stream", err)
		}
		for _, choice := range chunk.Choices {
			if choice.Delta == nil {
				continue
			}
			observed = true
			if choice.Delta.Content != "" {
				if !sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelTextDelta, Text: choice.Delta.Content}) {
					return false, ctx.Err()
				}
			}
			for _, delta := range choice.Delta.ToolCalls {
				toolFragments++
				if toolFragments > maxToolFragments {
					return false, fatalStreamError("provider tool fragment limit exceeded", nil)
				}
				if delta.Index < 0 {
					return false, fatalStreamError("provider tool index is negative", nil)
				}
				part := calls[delta.Index]
				if part == nil {
					if len(calls) >= maxToolIndexes {
						return false, fatalStreamError("provider tool index limit exceeded", nil)
					}
					part = &callParts{}
					calls[delta.Index] = part
				}
				if part.id.Len()+len(delta.ID) > maxStreamAggregateBytes || part.name.Len()+len(delta.Function.Name) > maxStreamAggregateBytes {
					return false, fatalStreamError("provider tool metadata exceeds 2 MiB", nil)
				}
				if part.arguments.Len()+len(delta.Function.Arguments) > maxStreamAggregateBytes {
					return false, fatalStreamError("provider tool arguments exceed 2 MiB", nil)
				}
				toolBytes += len(delta.ID) + len(delta.Function.Name) + len(delta.Function.Arguments)
				if toolBytes > maxStreamAggregateBytes {
					return false, fatalStreamError("provider tool data exceeds 2 MiB", nil)
				}
				part.id.WriteString(delta.ID)
				part.name.WriteString(delta.Function.Name)
				part.arguments.WriteString(delta.Function.Arguments)
			}
		}
		return false, nil
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		streamBytes += len(line) + 1
		if streamBytes > maxStreamTotalBytes {
			return observed, fatalStreamError("provider stream exceeds 10 MiB", nil)
		}
		if line == "" {
			done, err := dispatch()
			if err != nil || done {
				return observed, err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			field, value = line, ""
		}
		if field != "data" {
			continue
		}
		if strings.HasPrefix(value, " ") {
			value = value[1:]
		}
		dataFields++
		if dataFields > maxSSEDataFields {
			return observed, fatalStreamError("provider SSE data field limit exceeded", nil)
		}
		separatorBytes := 0
		if dataFields > 1 {
			separatorBytes = 1
		}
		if eventBytes+separatorBytes+len(value) > maxStreamAggregateBytes {
			return observed, fatalStreamError("provider SSE event exceeds 2 MiB", nil)
		}
		if separatorBytes > 0 {
			data.WriteByte('\n')
		}
		data.WriteString(value)
		eventBytes += separatorBytes + len(value)
	}
	if err := scanner.Err(); err != nil {
		return observed, interruptedError(err)
	}
	if done, err := dispatch(); err != nil || done {
		return observed, err
	}
	return observed, interruptedError(io.ErrUnexpectedEOF)
}

func emitCompletedCalls(ctx context.Context, out chan<- domain.ModelEvent, calls map[int]*callParts) error {
	indexes := make([]int, 0, len(calls))
	for index := range calls {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		if !json.Valid([]byte(calls[index].arguments.String())) {
			return interruptedError(fmt.Errorf("provider returned invalid tool arguments at index %d", index))
		}
	}
	for _, index := range indexes {
		part := calls[index]
		if !sendEvent(ctx, out, domain.ModelEvent{
			Kind: domain.ModelToolCall,
			ToolCall: &domain.ToolCall{
				ID:        part.id.String(),
				Name:      part.name.String(),
				Arguments: json.RawMessage(part.arguments.String()),
			},
		}) {
			return ctx.Err()
		}
	}
	return nil
}

func sendEvent(ctx context.Context, out chan<- domain.ModelEvent, event domain.ModelEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}
