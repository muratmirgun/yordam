package openaicompat_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
)

func TestStreamTextAndDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer key" {
			t.Errorf("authorization=%q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"hel"}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"lo"}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL, APIKey: "key", Model: "model-a"})
	events, err := client.Stream(context.Background(), domain.ModelRequest{Messages: []domain.Message{{Role: domain.RoleUser, Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	var text string
	var done bool
	for event := range events {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Kind == domain.ModelTextDelta {
			text += event.Text
		}
		if event.Kind == domain.ModelDone {
			done = true
		}
	}
	if text != "hello" || !done {
		t.Fatalf("text=%q done=%v", text, done)
	}
}

func TestStreamParsesSSEEventsRatherThanPhysicalLines(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": keepalive\r\n")
		fmt.Fprint(w, "event: message\r\n")
		fmt.Fprint(w, `data: {"choices":[`+"\r\n")
		fmt.Fprint(w, `data: {"delta":{"content":"multi"}}]}`+"\r\n")
		fmt.Fprint(w, "\r\n")
		fmt.Fprint(w, "id: ignored\r\n")
		fmt.Fprint(w, `data: {"choices":[{"delta":{"content":" event"}}]}`+"\r\n")
		fmt.Fprint(w, "\r\n")
		fmt.Fprint(w, "retry: 10\r\n")
		fmt.Fprint(w, ": final comment\r\n")
		fmt.Fprint(w, "data: [DONE]\r\n")
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL})
	events, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var text string
	var done bool
	for event := range events {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		text += event.Text
		done = done || event.Kind == domain.ModelDone
	}
	if text != "multi event" || !done {
		t.Fatalf("text=%q done=%v", text, done)
	}
}

func TestStreamRejectsMalformedMultilineSSEEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, `data: {"choices":[`)
		fmt.Fprintln(w, `data: not-json`)
		fmt.Fprintln(w)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient:  server.Client(),
		BaseURL:     server.URL,
		RetryDelays: []time.Duration{0, 0, 0},
	})
	events, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var streamErr error
	for event := range events {
		if event.Kind == domain.ModelStreamError {
			streamErr = event.Err
		}
	}
	var typed *domain.TypedError
	if !errors.As(streamErr, &typed) || typed.Kind != domain.ErrorProviderFatal {
		t.Fatalf("error=%v", streamErr)
	}
}

func TestStreamRejectsAggregateMultilineSSEEventOverLimit(t *testing.T) {
	line := strings.Repeat("x", 1024*1024)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(response, "data: %s\ndata: %s\ndata: %s\n\n", line, line, line)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL, RetryDelays: []time.Duration{}})
	stream, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var streamErr error
	for event := range stream {
		if event.Kind == domain.ModelStreamError {
			streamErr = event.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "SSE event exceeds") {
		t.Fatalf("error=%v", streamErr)
	}
}

func TestStreamRejectsExcessiveSSEDataFieldsAndEvents(t *testing.T) {
	tests := []struct {
		name string
		body func(http.ResponseWriter)
		want string
	}{
		{
			name: "data fields",
			body: func(response http.ResponseWriter) {
				for range 4097 {
					fmt.Fprintln(response, "data:")
				}
				fmt.Fprintln(response)
			},
			want: "data field limit",
		},
		{
			name: "events",
			body: func(response http.ResponseWriter) {
				for range 4097 {
					fmt.Fprintln(response, `data: {"choices":[]}`)
					fmt.Fprintln(response)
				}
			},
			want: "event limit",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "text/event-stream")
				test.body(response)
			}))
			defer server.Close()
			client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL, RetryDelays: []time.Duration{}})
			stream, err := client.Stream(context.Background(), domain.ModelRequest{})
			if err != nil {
				t.Fatal(err)
			}
			var streamErr error
			for event := range stream {
				if event.Kind == domain.ModelStreamError {
					streamErr = event.Err
				}
			}
			if streamErr == nil || !strings.Contains(streamErr.Error(), test.want) {
				t.Fatalf("error=%v want containing %q", streamErr, test.want)
			}
		})
	}
}

func TestStreamRejectsTotalBytesAcrossSmallEvents(t *testing.T) {
	payload := strings.Repeat("x", 64<<10)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		for range 200 {
			fmt.Fprintf(response, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", payload)
		}
	}))
	defer server.Close()
	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL, RetryDelays: []time.Duration{}})
	stream, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var streamErr error
	for event := range stream {
		if event.Kind == domain.ModelStreamError {
			streamErr = event.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "stream exceeds 10 MiB") {
		t.Fatalf("error=%v", streamErr)
	}
}

func TestRequestWireFormat(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/chat/completions" {
			t.Errorf("request=%s %s", request.Method, request.URL.Path)
		}
		if got := request.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type=%q", got)
		}
		if got := request.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("accept=%q", got)
		}
		var payload struct {
			Messages []struct {
				Role       string `json:"role"`
				Content    string `json:"content"`
				ToolCallID string `json:"tool_call_id"`
				ToolCalls  []struct {
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"messages"`
			Tools []struct {
				Type     string `json:"type"`
				Function struct {
					Name        string          `json:"name"`
					Description string          `json:"description"`
					Parameters  json.RawMessage `json:"parameters"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if len(payload.Messages) != 4 || payload.Messages[0].Role != "system" || payload.Messages[1].Role != "user" || payload.Messages[2].Role != "assistant" || payload.Messages[3].Role != "tool" {
			t.Fatalf("messages=%#v", payload.Messages)
		}
		assistant := payload.Messages[2]
		if assistant.Content != "calling" || len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].ID != "call-1" || assistant.ToolCalls[0].Type != "function" || assistant.ToolCalls[0].Function.Name != "read" || assistant.ToolCalls[0].Function.Arguments != `{"path":"a.go"}` {
			t.Fatalf("assistant=%#v", assistant)
		}
		if payload.Messages[3].ToolCallID != "call-1" || payload.Messages[3].Content != "result" {
			t.Fatalf("tool result=%#v", payload.Messages[3])
		}
		if len(payload.Tools) != 1 || payload.Tools[0].Type != "function" || payload.Tools[0].Function.Name != "read" || payload.Tools[0].Function.Description != "Read a file" || string(payload.Tools[0].Function.Parameters) != string(schema) {
			t.Fatalf("tools=%#v", payload.Tools)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL + "/v1/"})
	events, err := client.Stream(context.Background(), domain.ModelRequest{
		Messages: []domain.Message{
			{Role: domain.RoleSystem, Content: "system"},
			{Role: domain.RoleUser, Content: "question"},
			{Role: domain.RoleAssistant, Content: "calling", ToolCalls: []domain.ToolCall{{ID: "call-1", Name: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}},
			{Role: domain.RoleTool, Content: "result", ToolCallID: "call-1"},
		},
		Tools: []domain.ToolDescriptor{{Name: "read", Description: "Read a file", InputSchema: schema, Mutation: domain.MutationReadOnly}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for event := range events {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
}

func TestCancellationUnblocksUnconsumedStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan struct{})
	body := &trackingBody{
		Reader: strings.NewReader(`data: {"choices":[{"delta":{"content":"blocked"}}]}` + "\n\n"),
		closed: closed,
	}
	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return streamResponse(body), nil
		})},
		BaseURL: "http://provider.invalid",
	})
	events, err := client.Stream(ctx, domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("response body was not closed after cancellation")
	}
	if _, ok := <-events; ok {
		t.Fatal("stream emitted after cancellation while consumer was backpressured")
	}
}

type trackingBody struct {
	io.Reader
	closed chan struct{}
	once   sync.Once
}

func (body *trackingBody) Close() error {
	body.once.Do(func() { close(body.closed) })
	return nil
}
