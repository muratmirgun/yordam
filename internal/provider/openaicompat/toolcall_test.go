package openaicompat_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
)

func TestStreamAssemblesFragmentedToolCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"re","arguments":"{\"pa"}}]}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"ad","arguments":"th\":\"a.go\"}"}}]}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL, APIKey: "k", Model: "m"})
	events, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var call *domain.ToolCall
	for event := range events {
		if event.Kind == domain.ModelToolCall {
			call = event.ToolCall
		}
	}
	if call == nil || call.ID != "call-1" || call.Name != "read" || string(call.Arguments) != `{"path":"a.go"}` {
		t.Fatalf("call=%#v", call)
	}
}

func TestStreamEmitsToolCallsInIndexOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":2,"id":"third","function":{"name":"c","arguments":"{}"}},{"index":0,"id":"first","function":{"name":"a","arguments":"{}"}}]}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"second","function":{"name":"b","arguments":"{}"}}]}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL})
	events, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for event := range events {
		if event.Kind == domain.ModelToolCall {
			ids = append(ids, event.ToolCall.ID)
		}
	}
	if got := fmt.Sprint(ids); got != "[first second third]" {
		t.Fatalf("ids=%s", got)
	}
}

func TestStreamRejectsMalformedToolCallArguments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"read","arguments":"{"}}]}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL})
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
	if !errors.As(streamErr, &typed) || typed.Kind != domain.ErrorProviderInterrupted {
		t.Fatalf("error=%v", streamErr)
	}
}

func TestStreamValidatesAllToolCallsBeforeEmittingAny(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"valid","function":{"name":"read","arguments":"{}"}},{"index":1,"id":"invalid","function":{"name":"write","arguments":"{"}}]}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL})
	events, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var toolCalls int
	for event := range events {
		if event.Kind == domain.ModelToolCall {
			toolCalls++
		}
	}
	if toolCalls != 0 {
		t.Fatalf("tool calls=%d want 0", toolCalls)
	}
}

func TestStreamRejectsAggregateToolArgumentsOverLimit(t *testing.T) {
	fragment := strings.Repeat("x", 1024*1024)
	chunks := []string{
		fmt.Sprintf(`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"read","arguments":%q}}]}}]}`, `{"path":"`+fragment),
		fmt.Sprintf(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":%q}}]}}]}`, fragment),
		fmt.Sprintf(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":%q}}]}}]}`, fragment+`"}`),
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		for _, chunk := range chunks {
			fmt.Fprintf(response, "data: %s\n\n", chunk)
		}
		fmt.Fprint(response, "data: [DONE]\n\n")
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
	if streamErr == nil || !strings.Contains(streamErr.Error(), "tool arguments exceed") {
		t.Fatalf("error=%v", streamErr)
	}
}

func TestStreamRejectsAggregateToolArgumentsAcrossCalls(t *testing.T) {
	fragment := strings.Repeat("x", 800*1024)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		for index := range 3 {
			arguments := fmt.Sprintf(`{"path":%q}`, fragment)
			chunk := fmt.Sprintf(`{"choices":[{"delta":{"tool_calls":[{"index":%d,"id":"c%d","function":{"name":"read","arguments":%q}}]}}]}`, index, index, arguments)
			fmt.Fprintf(response, "data: %s\n\n", chunk)
		}
		fmt.Fprint(response, "data: [DONE]\n\n")
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
	if streamErr == nil || !strings.Contains(streamErr.Error(), "tool data exceeds") {
		t.Fatalf("error=%v", streamErr)
	}
}

func TestStreamRejectsExcessiveToolIndexesAndFragments(t *testing.T) {
	tests := []struct {
		name string
		body func(http.ResponseWriter)
		want string
	}{
		{
			name: "indexes",
			body: func(response http.ResponseWriter) {
				for index := range 129 {
					fmt.Fprintf(response, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":%d,\"id\":\"c%d\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]}}]}\n\n", index, index)
				}
			},
			want: "tool index limit",
		},
		{
			name: "fragments",
			body: func(response http.ResponseWriter) {
				for range 4097 {
					fmt.Fprintln(response, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":""}}]}}]}`)
					fmt.Fprintln(response)
				}
			},
			want: "tool fragment limit",
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
