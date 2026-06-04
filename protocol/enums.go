package protocol

// ThinkingLevel controls how much reasoning the model performs.
type ThinkingLevel string

const (
	ThinkingOff     ThinkingLevel = "off"
	ThinkingMinimal ThinkingLevel = "minimal"
	ThinkingLow     ThinkingLevel = "low"
	ThinkingMedium  ThinkingLevel = "medium"
	ThinkingHigh    ThinkingLevel = "high"
	ThinkingXHigh   ThinkingLevel = "xhigh"
)

// StopReason indicates why the model stopped generating.
type StopReason string

const (
	StopEnd    StopReason = "stop"     // Natural end
	StopLength StopReason = "length"   // Hit max tokens
	StopTool   StopReason = "toolUse"  // Wants to call a tool
	StopError  StopReason = "error"    // Error occurred
	StopAborted StopReason = "aborted" // Aborted by user
)

// CacheMode controls prompt caching behavior.
type CacheMode string

const (
	CacheNone CacheMode = "none"
	CacheShort CacheMode = "short"
	CacheLong  CacheMode = "long"
)

// Transport controls the streaming transport protocol.
type Transport string

const (
	TransportSSE             Transport = "sse"
	TransportWebSocket       Transport = "websocket"
	TransportWebSocketCached Transport = "websocket-cached"
	TransportAuto            Transport = "auto"
)

// MediaType describes input/output media types a model supports.
type MediaType string

const (
	MediaText  MediaType = "text"
	MediaImage MediaType = "image"
)
