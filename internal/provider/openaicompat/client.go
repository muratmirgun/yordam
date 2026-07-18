package openaicompat

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/secret"
)

type ClientOptions struct {
	HTTPClient  *http.Client
	BaseURL     string
	APIKey      string
	Model       string
	RetryDelays []time.Duration
	Jitter      func(time.Duration) time.Duration
	Redact      func(string) string
	Admission   *secret.Lease
}

type Client struct {
	http        *http.Client
	endpoint    string
	apiKey      string
	model       string
	retryDelays []time.Duration
	jitter      func(time.Duration) time.Duration
	redact      func(string) string
	admission   *secret.Lease
}

func New(opts ClientOptions) *Client {
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	if opts.RetryDelays == nil {
		opts.RetryDelays = []time.Duration{250 * time.Millisecond, time.Second, 2 * time.Second}
	}
	if len(opts.RetryDelays) > 3 {
		opts.RetryDelays = opts.RetryDelays[:3]
	}
	if opts.Jitter == nil {
		opts.Jitter = randomJitter
	}
	if opts.Redact == nil {
		opts.Redact = func(value string) string { return value }
	}
	if opts.Admission != nil {
		opts.Redact = opts.Admission.String
	}
	return &Client{
		http:        opts.HTTPClient,
		endpoint:    strings.TrimRight(opts.BaseURL, "/") + "/chat/completions",
		apiKey:      opts.APIKey,
		model:       opts.Model,
		retryDelays: append([]time.Duration(nil), opts.RetryDelays...),
		jitter:      opts.Jitter,
		redact:      opts.Redact,
		admission:   opts.Admission,
	}
}

func randomJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	half := delay / 2
	return half + time.Duration(rand.Int63n(int64(delay-half)+1))
}

func (c *Client) newRequest(ctx context.Context, input domain.ModelRequest) (*http.Request, error) {
	model := c.model
	if input.Selection.Model != "" {
		model = input.Selection.Model
	}
	payload := chatRequest{Model: model, Stream: true}
	for _, message := range input.Messages {
		wire := wireMessage{Role: string(message.Role), Content: message.Content, ToolCallID: message.ToolCallID}
		for _, call := range message.ToolCalls {
			wire.ToolCalls = append(wire.ToolCalls, wireToolCall{
				ID:       call.ID,
				Type:     "function",
				Function: wireFunction{Name: call.Name, Arguments: string(call.Arguments)},
			})
		}
		payload.Messages = append(payload.Messages, wire)
	}
	for _, tool := range input.Tools {
		payload.Tools = append(payload.Tools, wireTool{
			Type: "function",
			Function: wireFunction{
				Name:        tool.Name,
				Description: tool.Description,
				Parameters:  tool.InputSchema,
			},
		})
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	return req, nil
}

func (c *Client) openAttempt(ctx context.Context, input domain.ModelRequest) (*http.Response, error) {
	req, err := c.newRequest(ctx, input)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		kind := domain.ErrorProviderFatal
		if IsRetryable(err) {
			kind = domain.ErrorProviderRetryable
		}
		return nil, &domain.TypedError{Kind: kind, Message: "provider request failed", Cause: err}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, c.statusError(resp)
	}
	return resp, nil
}

func (c *Client) statusError(resp *http.Response) error {
	const maxErrorBodyBytes = 32 * 1024

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes+1))
	truncated := len(body) > maxErrorBodyBytes
	if truncated {
		body = body[:maxErrorBodyBytes]
	}
	rawBody := string(body)
	redactedBody := c.redact(rawBody)
	redactedBody = scrubSecretBoundary(redactedBody, c.apiKey, truncated || len(redactedBody) > maxErrorBodyBytes, maxErrorBodyBytes)
	if len(redactedBody) > maxErrorBodyBytes {
		redactedBody = redactedBody[:maxErrorBodyBytes]
	}
	status := &HTTPStatusError{
		Status:    resp.StatusCode,
		Body:      redactedBody,
		Truncated: truncated,
		ReadErr:   readErr,
	}
	kind := domain.ErrorProviderFatal
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		kind = domain.ErrorProviderRetryable
	} else if isContextTooLarge(resp.StatusCode, rawBody) {
		kind = domain.ErrorContextTooLarge
	}
	return &domain.TypedError{Kind: kind, Message: status.Error(), Cause: status}
}

func scrubSecretBoundary(body, secret string, crossesBoundary bool, limit int) string {
	if secret == "" {
		return body
	}
	body = strings.ReplaceAll(body, secret, "[redacted]")
	if !crossesBoundary {
		return body
	}
	retained := body
	if len(retained) > limit {
		retained = retained[:limit]
	}
	maxPrefix := min(len(secret)-1, len(retained))
	for size := maxPrefix; size > 0; size-- {
		if strings.HasSuffix(retained, secret[:size]) {
			return retained[:len(retained)-size] + "[redacted]"
		}
	}
	return body
}

func isContextTooLarge(status int, body string) bool {
	if status != http.StatusBadRequest && status != http.StatusRequestEntityTooLarge {
		return false
	}
	body = strings.ToLower(body)
	return strings.Contains(body, "context") &&
		strings.Contains(body, "token") &&
		(strings.Contains(body, "large") || strings.Contains(body, "long") || strings.Contains(body, "maximum"))
}
