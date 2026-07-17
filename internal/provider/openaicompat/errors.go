package openaicompat

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
)

type HTTPStatusError struct {
	Status    int
	Body      string
	Truncated bool
	ReadErr   error
}

func (e *HTTPStatusError) Error() string {
	var message strings.Builder
	fmt.Fprintf(&message, "provider returned HTTP %d", e.Status)
	if e.Body != "" {
		fmt.Fprintf(&message, ": %s", e.Body)
	}
	if e.Truncated {
		message.WriteString(" (truncated)")
	}
	if e.ReadErr != nil {
		message.WriteString(" (body read failed)")
	}
	return message.String()
}

func IsRetryable(err error) bool {
	var status *HTTPStatusError
	if errors.As(err, &status) {
		return status.Status == 429 || status.Status >= 500
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var network net.Error
	return errors.As(err, &network)
}

func interruptedError(cause error) error {
	return &domain.TypedError{
		Kind:    domain.ErrorProviderInterrupted,
		Message: "provider stream interrupted",
		Cause:   cause,
	}
}

func fatalStreamError(message string, cause error) error {
	return &domain.TypedError{
		Kind:    domain.ErrorProviderFatal,
		Message: message,
		Cause:   cause,
	}
}
