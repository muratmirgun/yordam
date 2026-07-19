package openaicompat_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
)

func TestRetryableHTTPStatusDoesNotRedispatch(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintln(w, "rate limited")
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient:  server.Client(),
		BaseURL:     server.URL,
		APIKey:      "k",
		Model:       "m",
		RetryDelays: []time.Duration{0, 0},
		Jitter:      func(delay time.Duration) time.Duration { return delay },
	})
	_, err := client.Stream(context.Background(), domain.ModelRequest{})
	if requests.Load() != 1 {
		t.Fatalf("requests=%d want 1", requests.Load())
	}
	var typed *domain.TypedError
	if !errors.As(err, &typed) || typed.Kind != domain.ErrorProviderRetryable {
		t.Fatalf("error=%v", err)
	}
}

func TestHTTPErrorBodyIsBoundedAndRedacted(t *testing.T) {
	const bodySecret = "body-secret"
	const authorizationSecret = "authorization-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, bodySecret+authorizationSecret+strings.Repeat("x", 32*1024)+"beyond-cap-marker")
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient: server.Client(),
		BaseURL:    server.URL,
		APIKey:     authorizationSecret,
		Redact: func(value string) string {
			return strings.ReplaceAll(value, bodySecret, "[redacted]")
		},
	})
	_, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err == nil {
		t.Fatal("expected HTTP error")
	}
	message := err.Error()
	for _, forbidden := range []string{bodySecret, authorizationSecret, "beyond-cap-marker"} {
		if strings.Contains(message, forbidden) {
			t.Fatalf("error exposes %q: %q", forbidden, message)
		}
	}
	if !strings.Contains(message, "[redacted]") || !strings.Contains(message, "truncated") {
		t.Fatalf("error=%q", message)
	}
}

func TestHTTPErrorBodyCapAndAPIKeyBoundary(t *testing.T) {
	const maxBody = 32 * 1024
	const boundaryMarker = "boundary-marker-value"
	tests := []struct {
		name          string
		body          string
		wantTruncated bool
		forbidden     string
	}{
		{name: "exact cap is not truncated", body: strings.Repeat("x", maxBody)},
		{name: "cap plus one is truncated", body: strings.Repeat("x", maxBody+1), wantTruncated: true},
		{name: "key crossing cap is scrubbed", body: strings.Repeat("x", maxBody-5) + boundaryMarker, wantTruncated: true, forbidden: boundaryMarker[:5]},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()

			client := openaicompat.New(openaicompat.ClientOptions{
				HTTPClient:  server.Client(),
				BaseURL:     server.URL,
				APIKey:      boundaryMarker,
				RetryDelays: []time.Duration{},
			})
			_, err := client.Stream(context.Background(), domain.ModelRequest{})
			var status *openaicompat.HTTPStatusError
			if !errors.As(err, &status) {
				t.Fatalf("error=%v", err)
			}
			if status.Truncated != test.wantTruncated || len(status.Body) > maxBody {
				t.Fatalf("truncated=%v body bytes=%d", status.Truncated, len(status.Body))
			}
			if test.forbidden != "" && strings.Contains(status.Body, test.forbidden) {
				t.Fatalf("body exposes key boundary %q", test.forbidden)
			}
		})
	}
}

func TestHTTPErrorBodyPreservesCustomRedactor(t *testing.T) {
	const redactionMarker = "custom-marker-value"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "prefix "+redactionMarker+" suffix")
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient:  server.Client(),
		BaseURL:     server.URL,
		APIKey:      redactionMarker,
		RetryDelays: []time.Duration{},
		Redact: func(value string) string {
			return strings.ReplaceAll(value, redactionMarker, "[custom-redacted]")
		},
	})
	_, err := client.Stream(context.Background(), domain.ModelRequest{})
	var status *openaicompat.HTTPStatusError
	if !errors.As(err, &status) || strings.Contains(status.Body, redactionMarker) || !strings.Contains(status.Body, "[custom-redacted]") {
		t.Fatalf("error=%v status=%#v", err, status)
	}
}

func TestContextTooLargeClassification(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		kind   domain.ErrorKind
	}{
		{name: "bad request", status: http.StatusBadRequest, body: "Maximum context token length reached", kind: domain.ErrorContextTooLarge},
		{name: "content too large", status: http.StatusRequestEntityTooLarge, body: "context token input is too large", kind: domain.ErrorContextTooLarge},
		{name: "missing token", status: http.StatusBadRequest, body: "context is too long", kind: domain.ErrorProviderFatal},
		{name: "wrong status", status: http.StatusUnprocessableEntity, body: "context token input is too large", kind: domain.ErrorProviderFatal},
		{name: "server status takes precedence", status: http.StatusInternalServerError, body: "context token input is too large", kind: domain.ErrorProviderRetryable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()

			client := openaicompat.New(openaicompat.ClientOptions{
				HTTPClient:  server.Client(),
				BaseURL:     server.URL,
				RetryDelays: []time.Duration{},
			})
			_, err := client.Stream(context.Background(), domain.ModelRequest{})
			var typed *domain.TypedError
			if !errors.As(err, &typed) || typed.Kind != test.kind {
				t.Fatalf("error=%v kind=%v want %v", err, typedKind(typed), test.kind)
			}
		})
	}
}

func TestRetryConfigurationDoesNotRedispatch(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	const retryDelay = 17 * time.Millisecond
	var jitterInput time.Duration
	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient:  server.Client(),
		BaseURL:     server.URL,
		RetryDelays: []time.Duration{retryDelay},
		Jitter: func(delay time.Duration) time.Duration {
			jitterInput = delay
			return 0
		},
	})
	_, err := client.Stream(context.Background(), domain.ModelRequest{})
	var typed *domain.TypedError
	if !errors.As(err, &typed) || typed.Kind != domain.ErrorProviderRetryable || jitterInput != 0 || requests.Load() != 1 {
		t.Fatalf("jitter input=%s requests=%d", jitterInput, requests.Load())
	}
}

func TestEOFWithoutDoneIsInterrupted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
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

func TestMalformedStreamIsNotRetried(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprintln(w, `data: {not-json}`)
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
	if requests.Load() != 1 || !errors.As(streamErr, &typed) || typed.Kind != domain.ErrorProviderFatal {
		t.Fatalf("requests=%d error=%v", requests.Load(), streamErr)
	}
}

func TestDecodeFailureAfterDeltaIsInterruptedOnce(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"partial"}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: {not-json}`)
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
	var streamErrors []error
	for event := range events {
		if event.Kind == domain.ModelStreamError {
			streamErrors = append(streamErrors, event.Err)
		}
	}
	var typed *domain.TypedError
	if requests.Load() != 1 || len(streamErrors) != 1 || !errors.As(streamErrors[0], &typed) || typed.Kind != domain.ErrorProviderInterrupted {
		t.Fatalf("requests=%d errors=%v", requests.Load(), streamErrors)
	}
}

func TestRetryStopsAfterMetadataDelta(t *testing.T) {
	var requests atomic.Int32
	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			body := io.MultiReader(
				strings.NewReader("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"),
				readError{err: fmt.Errorf("response body read: %w", io.ErrUnexpectedEOF)},
			)
			return streamResponse(body), nil
		})},
		BaseURL:     "http://provider.invalid",
		RetryDelays: []time.Duration{0, 0, 0},
		Jitter:      func(delay time.Duration) time.Duration { return delay },
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
	if requests.Load() != 1 || !errors.As(streamErr, &typed) || typed.Kind != domain.ErrorProviderInterrupted {
		t.Fatalf("requests=%d error=%v", requests.Load(), streamErr)
	}
}

func TestPreDeltaScannerFailureDoesNotRedispatch(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF},
		{name: "wrapped unexpected EOF", err: fmt.Errorf("response body read: %w", io.ErrUnexpectedEOF)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			client := openaicompat.New(openaicompat.ClientOptions{
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					requests.Add(1)
					return streamResponse(readError{err: test.err}), nil
				})},
				BaseURL:     "http://provider.invalid",
				RetryDelays: []time.Duration{0},
				Jitter:      func(delay time.Duration) time.Duration { return delay },
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
			if requests.Load() != 1 || !errors.As(streamErr, &typed) || typed.Kind != domain.ErrorProviderInterrupted {
				t.Fatalf("requests=%d error=%v", requests.Load(), streamErr)
			}
		})
	}
}

func TestCleanEOFBeforeDeltaDoesNotRedispatch(t *testing.T) {
	var requests atomic.Int32
	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return streamResponse(strings.NewReader("")), nil
		})},
		BaseURL:     "http://provider.invalid",
		RetryDelays: []time.Duration{0, 0, 0},
		Jitter:      func(delay time.Duration) time.Duration { return delay },
	})
	events, err := client.Stream(context.Background(), domain.ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var streamErrors []error
	for event := range events {
		if event.Kind == domain.ModelStreamError {
			streamErrors = append(streamErrors, event.Err)
		}
	}
	var typed *domain.TypedError
	if requests.Load() != 1 || len(streamErrors) != 1 || !errors.As(streamErrors[0], &typed) || typed.Kind != domain.ErrorProviderInterrupted {
		t.Fatalf("requests=%d errors=%v", requests.Load(), streamErrors)
	}
}

func TestTransportFailureDoesNotRedispatch(t *testing.T) {
	for _, transportErr := range []error{errorReader{}, timeoutReader{}} {
		var requests atomic.Int32
		client := openaicompat.New(openaicompat.ClientOptions{
			HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				requests.Add(1)
				return nil, transportErr
			})},
			BaseURL:     "http://provider.invalid",
			RetryDelays: []time.Duration{0, 0, 0, 0, 0},
			Jitter:      func(delay time.Duration) time.Duration { return delay },
		})
		_, err := client.Stream(context.Background(), domain.ModelRequest{})
		var typed *domain.TypedError
		if requests.Load() != 1 || !errors.As(err, &typed) || typed.Kind != domain.ErrorProviderRetryable {
			t.Fatalf("transport=%T requests=%d error=%v", transportErr, requests.Load(), err)
		}
	}
}

func TestCancellationDuringBackoffReturnsContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			cancel()
			return nil, errorReader{}
		})},
		BaseURL:     "http://provider.invalid",
		RetryDelays: []time.Duration{time.Hour},
		Jitter:      func(delay time.Duration) time.Duration { return delay },
	})
	_, err := client.Stream(ctx, domain.ModelRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func typedKind(err *domain.TypedError) domain.ErrorKind {
	if err == nil {
		return ""
	}
	return err.Kind
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errorReader{} }
func (errorReader) Error() string            { return "stream transport failed" }
func (errorReader) Timeout() bool            { return false }
func (errorReader) Temporary() bool          { return true }

type timeoutReader struct{}

func (timeoutReader) Error() string   { return "provider request timed out" }
func (timeoutReader) Timeout() bool   { return true }
func (timeoutReader) Temporary() bool { return true }

type readError struct{ err error }

func (reader readError) Read([]byte) (int, error) { return 0, reader.err }

func streamResponse(body io.Reader) *http.Response {
	closer, ok := body.(io.ReadCloser)
	if !ok {
		closer = io.NopCloser(body)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       closer,
	}
}
