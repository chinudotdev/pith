package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/protocol"
)

// OpenAICompatProvider implements ProviderPort for any OpenAI-compatible API
// (OpenAI, Groq, Together, DeepSeek, OpenRouter, local models, etc.).
//
// It speaks the /v1/chat/completions endpoint with SSE streaming.
type OpenAICompatProvider struct {
	api          protocol.ApiId
	name         string
	baseURL      string
	capabilities gateway.ProviderCapabilities
	httpClient   *http.Client
}

// OpenAICompatConfig configures the provider.
type OpenAICompatConfig struct {
	// API identifier (e.g. "openai-completions", "deepseek", "groq").
	// Defaults to "openai-completions".
	API protocol.ApiId

	// Name is a human-readable label. Defaults to "openai-compat".
	Name string

	// BaseURL is the API root (e.g. "https://api.openai.com", "https://api.groq.com").
	// The provider appends "/v1/chat/completions".
	BaseURL string

	// Capabilities declares what the provider supports.
	// Defaults to sensible OpenAI-compatible values.
	Capabilities gateway.ProviderCapabilities

	// HTTPClient overrides the default client. If nil, a 5-minute timeout client is used.
	HTTPClient *http.Client
}

// NewOpenAICompatProvider creates a provider for any OpenAI-compatible API.
func NewOpenAICompatProvider(cfg OpenAICompatConfig) *OpenAICompatProvider {
	api := cfg.API
	if api == "" {
		api = protocol.ApiOpenAICompletions
	}
	name := cfg.Name
	if name == "" {
		name = "openai-compat"
	}
	baseURL := strings.TrimRight(cfg.BaseURL, "/")

	caps := cfg.Capabilities
	// Apply sensible defaults only when the caller provides a zero-value
	// capabilities struct. If any field is explicitly set, respect it.
	if cfg.Capabilities == (gateway.ProviderCapabilities{}) {
		caps.MaxTokensField = gateway.MaxTokensCompletion
		caps.SupportsUsageInStreaming = true
		caps.CacheControlFormat = gateway.CacheControlNone
		caps.SupportsTemperature = true
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}

	return &OpenAICompatProvider{
		api:          api,
		name:         name,
		baseURL:      baseURL,
		capabilities: caps,
		httpClient:   client,
	}
}

func (p *OpenAICompatProvider) API() protocol.ApiId { return p.api }
func (p *OpenAICompatProvider) Name() string        { return p.name }
func (p *OpenAICompatProvider) Capabilities() gateway.ProviderCapabilities {
	return p.capabilities
}
func (p *OpenAICompatProvider) Initialize() error { return nil }
func (p *OpenAICompatProvider) Cleanup()          {}

// Stream makes a streaming request to /v1/chat/completions and returns
// a channel of protocol events parsed from the SSE response.
func (p *OpenAICompatProvider) Stream(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
	apiKey, err := resolveAPIKey(opts.Credential)
	if err != nil {
		return nil, err
	}

	body := p.buildRequest(model, pctx, opts)
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, &protocol.Error{Code: protocol.ErrInvalid, Message: fmt.Sprintf("failed to marshal request: %s", err)}
	}

	url := p.baseURL + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, &protocol.Error{Code: protocol.ErrUnknown, Message: fmt.Sprintf("failed to create request: %s", err)}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	for k, v := range opts.Headers {
		req.Header.Set(k, v)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, &protocol.Error{Code: protocol.ErrTimeout, Message: fmt.Sprintf("request failed: %s", err), Cause: err}
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		code := mapHTTPStatus(resp.StatusCode)
		return nil, &protocol.Error{
			Code:    code,
			Message: fmt.Sprintf("API returned %d: %s", resp.StatusCode, string(respBody)),
		}
	}

	return p.parseSSEStream(ctx, resp.Body, model)
}

// buildRequest constructs the OpenAI chat completions request body.
func (p *OpenAICompatProvider) buildRequest(model protocol.ModelDescriptor, ctx protocol.Context, opts protocol.StreamOptions) map[string]any {
	messages := make([]map[string]any, 0, len(ctx.Messages)+1)

	if ctx.SystemPrompt != "" {
		role := "system"
		if p.capabilities.SupportsDeveloperRole {
			role = "developer"
		}
		messages = append(messages, map[string]any{
			"role":    role,
			"content": ctx.SystemPrompt,
		})
	}

	for _, msg := range ctx.Messages {
		switch m := msg.(type) {
		case protocol.UserMessage:
			messages = append(messages, map[string]any{
				"role":    "user",
				"content": contentToOpenAI(m.Content),
			})
		case protocol.AssistantMessage:
			entry := map[string]any{"role": "assistant"}
			content := make([]map[string]any, 0, len(m.Content))
			var toolCalls []map[string]any
			for _, block := range m.Content {
				switch b := block.(type) {
				case protocol.TextContent:
					content = append(content, map[string]any{"type": "text", "text": b.Text})
				case protocol.ThinkingContent:
					if p.capabilities.RequiresThinkingAsText {
						content = append(content, map[string]any{"type": "text", "text": b.Thinking})
					} else {
						content = append(content, map[string]any{"type": "thinking", "thinking": b.Thinking})
					}
				case protocol.ToolCall:
					var args any
					if err := json.Unmarshal([]byte(b.Arguments), &args); err != nil {
						args = b.Arguments
					}
					toolCalls = append(toolCalls, map[string]any{
						"id": b.ID, "type": "function", "function": map[string]any{
							"name": b.Name, "arguments": b.Arguments,
						},
					})
				}
			}
			if len(content) == 1 && content[0]["type"] == "text" {
				entry["content"] = content[0]["text"]
			} else if len(content) > 0 {
				entry["content"] = content
			}
			if len(toolCalls) > 0 {
				entry["tool_calls"] = toolCalls
			}
			messages = append(messages, entry)

		case protocol.ToolResultMessage:
			entry := map[string]any{"role": "tool"}
			if p.capabilities.RequiresToolResultName {
				entry["name"] = m.ToolName
			}
			entry["tool_call_id"] = m.ToolCallID
			entry["content"] = contentToOpenAI(m.Content)
			messages = append(messages, entry)

		case protocol.CompactSummaryMessage:
			messages = append(messages, map[string]any{
				"role": "user", "content": "[Previous conversation summary]\n" + m.Summary,
			})
		}
	}

	body := map[string]any{
		"model":    model.ID,
		"messages": messages,
		"stream":   true,
	}

	// Request usage stats in the final chunk (OpenAI requires stream_options)
	if p.capabilities.SupportsUsageInStreaming {
		body["stream_options"] = map[string]any{"include_usage": true}
	}

	if len(ctx.Tools) > 0 {
		tools := make([]map[string]any, 0, len(ctx.Tools))
		for _, t := range ctx.Tools {
			tools = append(tools, map[string]any{
				"type": "function",
				"function": map[string]any{
					"name": t.Name, "description": t.Description, "parameters": t.Parameters,
				},
			})
		}
		body["tools"] = tools
	}

	if opts.MaxTokens != nil {
		field := "max_tokens"
		if p.capabilities.MaxTokensField == gateway.MaxTokensCompletion {
			field = "max_completion_tokens"
		}
		body[field] = *opts.MaxTokens
	} else if model.MaxTokens > 0 {
		field := "max_tokens"
		if p.capabilities.MaxTokensField == gateway.MaxTokensCompletion {
			field = "max_completion_tokens"
		}
		body[field] = model.MaxTokens
	}

	if opts.Temperature != nil {
		body["temperature"] = *opts.Temperature
	}

	if opts.Reasoning != "" && opts.Reasoning != protocol.ThinkingOff {
		if p.capabilities.SupportsReasoningEffort {
			body["reasoning_effort"] = string(opts.Reasoning)
		}
	}

	return body
}

// parseSSEStream reads the SSE response body and emits protocol events.
func (p *OpenAICompatProvider) parseSSEStream(ctx context.Context, body io.ReadCloser, model protocol.ModelDescriptor) (<-chan protocol.StreamEvent, error) {
	ch := make(chan protocol.StreamEvent, 64)

	// Ensure body is closed exactly once, whether by normal completion
	// or by context cancellation.
	var closeBody sync.Once
	doClose := func() { closeBody.Do(func() { body.Close() }) }

	// When the context is cancelled, close the response body to unblock
	// the scanner. This ensures streaming stops promptly on abort.
	go func() {
		<-ctx.Done()
		doClose()
	}()

	go func() {
		defer doClose()
		defer close(ch)

		partial := &protocol.AssistantMessage{
			Role:      "assistant",
			API:       model.API,
			Provider:  model.Provider,
			Model:     model.ID,
			Timestamp: protocol.Now(),
		}

		textStarted := false
		textContentIndex := -1
		var toolCalls []protocol.ToolCall
		var accumulatedText strings.Builder

		snapshotPartial := func() *protocol.AssistantMessage {
			snap := *partial
			snap.Content = make([]protocol.ContentBlock, len(partial.Content))
			copy(snap.Content, partial.Content)
			return &snap
		}

		ch <- protocol.StreamEvent{Type: protocol.EventStart, Partial: snapshotPartial()}

		scanner := bufio.NewScanner(body)
		for scanner.Scan() {
			// Check for context cancellation between reads
			select {
			case <-ctx.Done():
				ch <- protocol.StreamEvent{
					Type:    protocol.EventError,
					Reason:  protocol.StopAborted,
					Message: snapshotPartial(),
				}
				return
			default:
			}

			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")

			if data == "[DONE]" {
				// Finalize text if still streaming
				if textStarted {
					partial.Content = append(partial.Content, protocol.TextContent{Type: "text", Text: accumulatedText.String()})
					ch <- protocol.StreamEvent{
						Type: protocol.EventTextEnd, ContentIndex: textContentIndex,
						Content: accumulatedText.String(), Partial: snapshotPartial(),
					}
				}
				// Finalize tool calls
				for i, tc := range toolCalls {
					partial.Content = append(partial.Content, tc)
					ch <- protocol.StreamEvent{
						Type: protocol.EventToolCallEnd, ContentIndex: len(partial.Content) - 1,
						ToolCall: &toolCalls[i], Partial: snapshotPartial(),
					}
				}

				stopReason := partial.StopReason
				if stopReason == "" {
					stopReason = protocol.StopEnd
				}
				ch <- protocol.StreamEvent{
					Type: protocol.EventDone, Reason: stopReason,
					Message: snapshotPartial(), Partial: snapshotPartial(),
				}
				return
			}

			var chunk openAIChunk
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				continue
			}

			// Usage can arrive in a chunk with empty choices (OpenAI stream_options)
			if chunk.Usage != nil {
				partial.Usage = protocol.Usage{
					Input: chunk.Usage.PromptTokens, Output: chunk.Usage.CompletionTokens,
					TotalTokens: chunk.Usage.TotalTokens,
				}
			}

			if len(chunk.Choices) == 0 {
				continue
			}
			choice := chunk.Choices[0]

			if choice.Delta != nil {
				// Text content
				if choice.Delta.Content != "" {
					if !textStarted {
						textContentIndex = len(partial.Content)
						textStarted = true
						ch <- protocol.StreamEvent{
							Type: protocol.EventTextStart, ContentIndex: textContentIndex,
							Partial: snapshotPartial(),
						}
					}
					accumulatedText.WriteString(choice.Delta.Content)
					ch <- protocol.StreamEvent{
						Type: protocol.EventTextDelta, ContentIndex: textContentIndex,
						Delta: choice.Delta.Content, Partial: snapshotPartial(),
					}
				}

				// Tool calls
				for _, tc := range choice.Delta.ToolCalls {
					for len(toolCalls) <= tc.Index {
						toolCalls = append(toolCalls, protocol.ToolCall{Type: "toolCall"})
					}
					if tc.ID != "" {
						toolCalls[tc.Index].ID = tc.ID
					}
					if tc.Function.Name != "" {
						toolCalls[tc.Index].Name = tc.Function.Name
					}
					toolCalls[tc.Index].Arguments += tc.Function.Arguments

					contentIdx := len(partial.Content) + tc.Index
					if tc.ID != "" || tc.Function.Name != "" {
						ch <- protocol.StreamEvent{
							Type: protocol.EventToolCallStart, ContentIndex: contentIdx,
							Partial: snapshotPartial(),
						}
					}
					if tc.Function.Arguments != "" {
						ch <- protocol.StreamEvent{
							Type: protocol.EventToolCallDelta, ContentIndex: contentIdx,
							Delta: tc.Function.Arguments, Partial: snapshotPartial(),
						}
					}
				}

				// Reasoning content
				if choice.Delta.ReasoningContent != "" {
					ch <- protocol.StreamEvent{
						Type: protocol.EventThinkingDelta, ContentIndex: len(partial.Content),
						Delta: choice.Delta.ReasoningContent, Partial: snapshotPartial(),
					}
				}
			}

			if choice.FinishReason != "" {
				partial.StopReason = mapFinishReason(choice.FinishReason)
			}
		}

		// Check if the scanner exited due to context cancellation
		if ctx.Err() != nil {
			ch <- protocol.StreamEvent{
				Type:    protocol.EventError,
				Reason:  protocol.StopAborted,
				Message: snapshotPartial(),
			}
			return
		}

		// EOF without [DONE] — emit Done anyway
		if partial.StopReason == "" {
			partial.StopReason = protocol.StopEnd
		}
		ch <- protocol.StreamEvent{
			Type: protocol.EventDone, Reason: partial.StopReason,
			Message: snapshotPartial(), Partial: snapshotPartial(),
		}
	}()

	return ch, nil
}

// --- OpenAI JSON types ---

type openAIChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Choices []openAIChoice `json:"choices"`
	Usage   *openAIUsage   `json:"usage,omitempty"`
}

type openAIChoice struct {
	Index        int          `json:"index"`
	Delta        *openAIDelta `json:"delta,omitempty"`
	FinishReason string       `json:"finish_reason,omitempty"`
}

type openAIDelta struct {
	Role             string           `json:"role,omitempty"`
	Content          string           `json:"content,omitempty"`
	ReasoningContent string           `json:"reasoning_content,omitempty"`
	ToolCalls        []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAIToolCall struct {
	Index    int                `json:"index"`
	ID       string             `json:"id,omitempty"`
	Type     string             `json:"type,omitempty"`
	Function openAIFunctionCall `json:"function"`
}

type openAIFunctionCall struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// --- Helpers ---

func resolveAPIKey(cred protocol.Credential) (string, error) {
	if cred == nil {
		return "", &protocol.Error{Code: protocol.ErrAuth, Message: "no credential provided"}
	}
	switch c := cred.(type) {
	case protocol.ApiKey:
		return c.Key, nil
	case protocol.BearerToken:
		return c.Token, nil
	default:
		return "", &protocol.Error{Code: protocol.ErrAuth, Message: fmt.Sprintf("unsupported credential type %T for openai-compat provider", cred)}
	}
}

func contentToOpenAI(content []protocol.Content) any {
	if len(content) == 1 {
		if tc, ok := content[0].(protocol.TextContent); ok {
			return tc.Text
		}
	}
	parts := make([]map[string]any, 0, len(content))
	for _, c := range content {
		switch v := c.(type) {
		case protocol.TextContent:
			parts = append(parts, map[string]any{"type": "text", "text": v.Text})
		case protocol.ImageContent:
			parts = append(parts, map[string]any{
				"type": "image_url",
				"image_url": map[string]any{
					"url": "data:" + v.MimeType + ";base64," + v.Data,
				},
			})
		}
	}
	return parts
}

func mapFinishReason(reason string) protocol.StopReason {
	switch reason {
	case "stop":
		return protocol.StopEnd
	case "length":
		return protocol.StopLength
	case "tool_calls", "tool_use":
		return protocol.StopTool
	default:
		return protocol.StopEnd
	}
}

func mapHTTPStatus(status int) protocol.ErrorCode {
	switch {
	case status == 401 || status == 403:
		return protocol.ErrAuth
	case status == 404:
		return protocol.ErrNotFound
	case status == 429:
		return protocol.ErrRateLimited
	case status >= 500:
		return protocol.ErrTimeout
	default:
		return protocol.ErrUnknown
	}
}

// Verify interface compliance at compile time.
var _ gateway.ProviderPort = (*OpenAICompatProvider)(nil)
