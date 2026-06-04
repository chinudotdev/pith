package protocol

import "time"

// --- Context ---

// Context is the input to an LLM call: system prompt + messages + tools.
type Context struct {
	SystemPrompt string    `json:"systemPrompt,omitempty"`
	Messages     []Message `json:"messages"`
	Tools        []Tool    `json:"tools,omitempty"`
}

// --- Stream Options ---

// ThinkingBudgets configures per-level token budgets for thinking.
type ThinkingBudgets struct {
	Minimal *int `json:"minimal,omitempty"`
	Low     *int `json:"low,omitempty"`
	Medium  *int `json:"medium,omitempty"`
	High    *int `json:"high,omitempty"`
}

// StreamOptions controls a single LLM streaming request.
// No API key field — credentials are resolved by CredentialProvider in L1.
type StreamOptions struct {
	Temperature              *float64         `json:"temperature,omitempty"`
	MaxTokens                *int             `json:"maxTokens,omitempty"`
	Signal                   <-chan struct{}  `json:"-"` // abort signal
	Transport                Transport        `json:"transport,omitempty"`
	CacheRetention           CacheMode        `json:"cacheRetention,omitempty"`
	SessionID                string           `json:"sessionId,omitempty"`
	Headers                  map[string]string `json:"headers,omitempty"`
	TimeoutMs                *int             `json:"timeoutMs,omitempty"`
	WebsocketConnectTimeoutMs *int            `json:"websocketConnectTimeoutMs,omitempty"`
	MaxRetries               *int             `json:"maxRetries,omitempty"`
	MaxRetryDelayMs          *int             `json:"maxRetryDelayMs,omitempty"`
	Metadata                 map[string]any   `json:"metadata,omitempty"`
	Reasoning                ThinkingLevel    `json:"reasoning,omitempty"`
	ThinkingBudgets          *ThinkingBudgets `json:"thinkingBudgets,omitempty"`

	// Resolved credential, set by L1 gateway before calling the provider.
	// Providers read this instead of parsing Metadata.
	Credential Credential `json:"-"`

	// Callbacks (not serialized)
	OnPayload  func(payload map[string]any) map[string]any `json:"-"`
	OnResponse func(status int, headers map[string]string)  `json:"-"`
}

// ProviderResponse captures the HTTP response metadata from a provider.
type ProviderResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
}

// --- Error ---

// ErrorCode is a domain-specific error code string.
type ErrorCode = string

// Known error codes.
const (
	ErrNotFound          ErrorCode = "not_found"
	ErrPermissionDenied  ErrorCode = "permission_denied"
	ErrTimeout           ErrorCode = "timeout"
	ErrRateLimited       ErrorCode = "rate_limited"
	ErrAborted           ErrorCode = "aborted"
	ErrInvalid           ErrorCode = "invalid"
	ErrNotSupported      ErrorCode = "not_supported"
	ErrUnknown           ErrorCode = "unknown"
	ErrNotDirectory      ErrorCode = "not_directory"
	ErrIsDirectory       ErrorCode = "is_directory"
	ErrSpawnError        ErrorCode = "spawn_error"
	ErrSummarization     ErrorCode = "summarization_failed"
	ErrInvalidSession    ErrorCode = "invalid_session"
	ErrStorage           ErrorCode = "storage"
	ErrBusy              ErrorCode = "busy"
	ErrInvalidState      ErrorCode = "invalid_state"
	ErrInvalidArgument   ErrorCode = "invalid_argument"
	ErrAuth              ErrorCode = "auth"
	ErrHook              ErrorCode = "hook"
)

// Error is a structured error with code, message, and optional cause.
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Cause   error     `json:"-"`
	Path    string    `json:"path,omitempty"`
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.Cause }

// NewError creates a new Error.
func NewError(code ErrorCode, message string) *Error {
	return &Error{Code: code, Message: message}
}

// NewErrorWithCause creates a new Error with a cause.
func NewErrorWithCause(code ErrorCode, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Cause: cause}
}

// --- Timestamp helper ---

// Now returns the current UTC timestamp. It is a package-level variable
// so tests can override it with a fixed clock. Do not override in production.
var Now = func() time.Time {
	return time.Now().UTC()
}
