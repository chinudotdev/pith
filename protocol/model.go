package protocol

// --- Tool Definition ---

// Tool describes a tool that the model can invoke.
type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"` // JSON Schema object
}

// --- Model Descriptor ---

// ModelDescriptor fully describes a model available through the gateway.
type ModelDescriptor struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	API          ApiId             `json:"api"`
	Provider     ProviderId        `json:"provider"`
	BaseURL      string            `json:"baseUrl"`
	Capabilities ModelCapabilities `json:"capabilities"`
	Cost         CostRates         `json:"cost"`
	ContextWindow int              `json:"contextWindow"`
	MaxTokens    int               `json:"maxTokens"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// ModelCapabilities describes what a model can do.
type ModelCapabilities struct {
	Input          map[MediaType]bool `json:"input"`
	Thinking       bool               `json:"thinking"`
	ThinkingLevels map[ThinkingLevel]bool `json:"thinkingLevels"`
	DefaultThinkingLevel ThinkingLevel `json:"defaultThinkingLevel,omitempty"` // model's default when ThinkingLevel is not explicitly set
	Caching        map[CacheMode]bool `json:"caching"`
	Transport      map[Transport]bool `json:"transport"`
}

// CostRates describes per-token pricing in dollars per million tokens.
type CostRates struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
}

// --- Usage & Cost ---

// Usage tracks token consumption for a single request.
type Usage struct {
	Input       int     `json:"input"`
	Output      int     `json:"output"`
	CacheRead   int     `json:"cacheRead"`
	CacheWrite  int     `json:"cacheWrite"`
	TotalTokens int     `json:"totalTokens"`
	Cost        Cost    `json:"cost"`
}

// Cost tracks dollar amounts for a single request.
type Cost struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
	Total      float64 `json:"total"`
}

// --- Diagnostic ---

// Diagnostic provides non-fatal information about a request.
type Diagnostic struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Redacted bool   `json:"redacted"`
	Provider string `json:"provider,omitempty"`
}
