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
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
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

func (c *Client) streamOnce(ctx context.Context, input domain.ModelRequest) (<-chan domain.ModelEvent, error) {
	resp, err := c.openAttempt(ctx, input)
	if err != nil {
		return nil, err
	}
	out := make(chan domain.ModelEvent)
	go func() {
		defer close(out)
		_, consumeErr := c.consumeAttempt(ctx, resp, out)
		if consumeErr != nil && ctx.Err() == nil {
			sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelStreamError, Err: consumeErr})
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

type streamMetadata struct {
	ID      string     `json:"id"`
	Usage   *wireUsage `json:"usage"`
	Choices []struct {
		Delta *struct {
			Refusal string `json:"refusal"`
		} `json:"delta"`
	} `json:"choices"`
}

type wireUsage struct {
	PromptTokens     *int64 `json:"prompt_tokens"`
	CompletionTokens *int64 `json:"completion_tokens"`
	PromptDetails    *struct {
		CachedTokens *int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionDetails *struct {
		ReasoningTokens *int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

func (c *Client) consumeAttempt(ctx context.Context, resp *http.Response, out chan<- domain.ModelEvent) (bool, error) {
	defer resp.Body.Close()
	var redaction *secret.LeasedRedactionStream
	if c.admission != nil {
		var err error
		redaction, err = c.admission.RedactionStream()
		if err != nil {
			return false, fatalStreamError("acquire provider admission stream", err)
		}
		defer redaction.Close()
	}
	flushText := func() bool {
		if redaction == nil {
			return true
		}
		value := redaction.Close()
		return value == "" || sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelTextDelta, Text: value})
	}

	observed := false
	serverRequestID := resp.Header.Get("X-Request-ID")
	if serverRequestID == "" {
		serverRequestID = resp.Header.Get("Request-ID")
	}
	finishReason := ""
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
			if !flushText() {
				return true, ctx.Err()
			}
			if err := emitCompletedCalls(ctx, out, calls, c.admission); err != nil {
				return true, err
			}
			if !sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelDone, RequestID: serverRequestID, FinishReason: finishReason}) {
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
		var metadata streamMetadata
		if err := json.Unmarshal([]byte(encoded), &metadata); err != nil {
			return false, fatalStreamError("decode provider stream metadata", err)
		}
		if metadata.ID != "" {
			serverRequestID = metadata.ID
		}
		if metadata.Usage != nil {
			observed = true
			usage := normalizeUsage(*metadata.Usage)
			if !sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelUsageUpdate, Usage: &usage, RequestID: serverRequestID}) {
				return false, ctx.Err()
			}
		}
		for choiceIndex, choice := range chunk.Choices {
			if choice.FinishReason != nil {
				observed = true
				finishReason = *choice.FinishReason
			}
			if choice.Delta == nil {
				continue
			}
			observed = true
			if choice.Delta.Content != "" {
				content := choice.Delta.Content
				if redaction != nil {
					content = redaction.Write(content)
				}
				if content != "" && !sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelTextDelta, Text: content}) {
					return false, ctx.Err()
				}
			}
			refusalDelta := ""
			if choiceIndex < len(metadata.Choices) && metadata.Choices[choiceIndex].Delta != nil {
				refusalDelta = metadata.Choices[choiceIndex].Delta.Refusal
			}
			if refusalDelta != "" {
				refusal := c.redact(refusalDelta)
				if refusal != "" && !sendEvent(ctx, out, domain.ModelEvent{Kind: domain.ModelRefusalDelta, Refusal: refusal, RequestID: serverRequestID}) {
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

func normalizeUsage(usage wireUsage) protocol.ModelUsage {
	providerValue := func(value *int64) protocol.UsageValue {
		if value == nil {
			return protocol.UsageValue{State: protocol.UsageUnknown, Provenance: "openai_compatible"}
		}
		return protocol.UsageValue{State: protocol.UsageProviderReported, Value: *value, Provenance: "openai_compatible"}
	}
	unknown := protocol.UsageValue{State: protocol.UsageUnknown, Provenance: "openai_compatible"}
	var cached, reasoning *int64
	if usage.PromptDetails != nil {
		cached = usage.PromptDetails.CachedTokens
	}
	if usage.CompletionDetails != nil {
		reasoning = usage.CompletionDetails.ReasoningTokens
	}
	return protocol.ModelUsage{
		Input: providerValue(usage.PromptTokens), Output: providerValue(usage.CompletionTokens),
		Cached: providerValue(cached), CacheWrite: unknown, Reasoning: providerValue(reasoning),
	}
}

func emitCompletedCalls(ctx context.Context, out chan<- domain.ModelEvent, calls map[int]*callParts, admission *secret.Lease) error {
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
		id, name := part.id.String(), part.name.String()
		arguments := json.RawMessage(part.arguments.String())
		if admission != nil {
			id = admission.String(id)
			name = admission.String(name)
			var value any
			decoder := json.NewDecoder(strings.NewReader(string(arguments)))
			decoder.UseNumber()
			if err := decoder.Decode(&value); err != nil {
				return interruptedError(fmt.Errorf("decode provider tool arguments at index %d", index))
			}
			redacted, err := admission.JSON(value)
			if err != nil {
				return interruptedError(fmt.Errorf("redact provider tool arguments at index %d", index))
			}
			arguments = redacted
		}
		if !sendEvent(ctx, out, domain.ModelEvent{
			Kind: domain.ModelToolCall,
			ToolCall: &domain.ToolCall{
				ID:        id,
				Name:      name,
				Arguments: arguments,
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
