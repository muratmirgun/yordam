package domain

type ErrorKind string

const (
	ErrorCancelled            ErrorKind = "cancelled"
	ErrorPermissionDenied     ErrorKind = "permission_denied"
	ErrorToolFailed           ErrorKind = "tool_failed"
	ErrorToolTimeout          ErrorKind = "tool_timeout"
	ErrorProviderRetryable    ErrorKind = "provider_retryable"
	ErrorProviderInterrupted  ErrorKind = "provider_interrupted"
	ErrorProviderFatal        ErrorKind = "provider_fatal"
	ErrorContextTooLarge      ErrorKind = "context_too_large"
	ErrorStorageCorrupt       ErrorKind = "storage_corrupt"
	ErrorConfigurationInvalid ErrorKind = "configuration_invalid"
)

type TypedError struct {
	Kind    ErrorKind
	Message string
	Cause   error
}

func (e *TypedError) Error() string { return e.Message }
func (e *TypedError) Unwrap() error { return e.Cause }
