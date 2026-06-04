// Example 11: Custom Provider — build your own ProviderPort for an API
// the SDK doesn't ship with. This example implements the Anthropic Messages API
// (SSE streaming) and wires it through the gateway and agent.
//
// This demonstrates that:
//   - ProviderPort is the extension point for any LLM API
//   - The gateway, catalog, and agent don't need to know about your provider
//   - You can mix custom providers with built-in ones in the same gateway
//
//	mkdir my-agent && cd my-agent && go mod init my-agent
//	cp main.go . && go mod tidy
//	ANTHROPIC_API_KEY="sk-ant-..." go run main.go
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/chinudotdev/pith/agent"
	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/protocol"
)

// ---------------------------------------------------------------------------
// AnthropicProvider — a custom ProviderPort implementation
// ---------------------------------------------------------------------------

// AnthropicProvider implements gateway.ProviderPort for the Anthropic Messages API.
// It speaks the /v1/messages endpoint with SSE streaming.
type AnthropicProvider struct {
	baseURL    string
	httpClient *http.Client
}

// AnthropicConfig configures the Anthropic provider.
type AnthropicConfig struct {
	BaseURL    string       // Defaults to "https://api.anthropic.com"
	HTTPClient *http.Client // Defaults to 5-minute timeout
}

// NewAnthropicProvider creates a provider for the Anthropic Messages API.
func NewAnthropicProvider(cfg AnthropicConfig) *AnthropicProvider {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.anthropic.com"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	return &AnthropicProvider{baseURL: baseURL, httpClient: client}
}

// --- ProviderPort interface ---

func (p *AnthropicProvider) API() protocol.ApiId    { return protocol.ApiAnthropicMessages }
func (p *AnthropicProvider) Name() string           { return "anthropic" }
func (p *AnthropicProvider) Initialize() error      { return nil }
func (p *AnthropicProvider) Cleanup()               {}

func (p *AnthropicProvider) Capabilities() gateway.ProviderCapabilities {
	return gateway.ProviderCapabilities{
		MaxTokensField:                  gateway.MaxTokensLegacy,
		SupportsTemperature:             true,
		CacheControlFormat:              gateway.CacheControlAnthropic,
		SupportsCacheControlOnTools:     true,
		SupportsEagerToolInputStreaming:  true,
		ThinkingFormat:                   gateway.ThinkingAntLing,
		ForceAdaptiveThinking:            true,
		AllowEmptySignature:              true,
	}
}

// Stream makes a streaming request to /v1/messages.
func (p *AnthropicProvider) Stream(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
	apiKey, err := resolveAnthropicKey(opts.Credential)
	if err != nil {
		return nil, err
	}

	body := p.buildRequest(model, pctx, opts)
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, &protocol.Error{Code: protocol.ErrInvalid, Message: fmt.Sprintf("failed to marshal request: %s", err)}
	}

	url := p.baseURL + "/v1/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, &protocol.Error{Code: protocol.ErrUnknown, Message: fmt.Sprintf("failed to create request: %s", err)}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
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
		return nil, &protocol.Error{
			Code:    mapHTTPStatus(resp.StatusCode),
			Message: fmt.Sprintf("Anthropic API returned %d: %s", resp.StatusCode, string(respBody)),
		}
	}

	return p.parseSSEStream(ctx, resp.Body, model)
}

// buildRequest constructs the Anthropic Messages API request body.
func (p *AnthropicProvider) buildRequest(model protocol.ModelDescriptor, ctx protocol.Context, opts protocol.StreamOptions) map[string]any {
	body := map[string]any{
		"model":      model.ID,
		"stream":     true,
		"max_tokens": 4096,
	}

	if ctx.SystemPrompt != "" {
		body["system"] = ctx.SystemPrompt
	}

	if opts.MaxTokens != nil {
		body["max_tokens"] = *opts.MaxTokens
	} else if model.MaxTokens > 0 {
		body["max_tokens"] = model.MaxTokens
	}

	if opts.Temperature != nil {
		body["temperature"] = *opts.Temperature
	}

	// Build messages (no system role in Anthropic — it's a top-level field)
	messages := make([]map[string]any, 0, len(ctx.Messages))
	for _, msg := range ctx.Messages {
		switch m := msg.(type) {
		case protocol.UserMessage:
			messages = append(messages, map[string]any{
				"role":    "user",
				"content": contentToAnthropic(m.Content),
			})

		case protocol.AssistantMessage:
			entry := map[string]any{"role": "assistant"}
			content := make([]map[string]any, 0, len(m.Content))
			for _, block := range m.Content {
				switch b := block.(type) {
				case protocol.TextContent:
					content = append(content, map[string]any{"type": "text", "text": b.Text})
				case protocol.ThinkingContent:
					tb := map[string]any{"type": "thinking", "thinking": b.Thinking}
					if b.ThinkingSignature != "" {
						tb["signature"] = b.ThinkingSignature
					}
					content = append(content, tb)
				case protocol.ToolCall:
					content = append(content, map[string]any{
						"type":  "tool_use",
						"id":    b.ID,
						"name":  b.Name,
						"input": json.RawMessage(b.Arguments),
					})
				}
			}
			if len(content) > 0 {
				entry["content"] = content
			}
			messages = append(messages, entry)

		case protocol.ToolResultMessage:
			messages = append(messages, map[string]any{
				"role": "user",
				"content": []map[string]any{{
					"type":        "tool_result",
					"tool_use_id": m.ToolCallID,
					"content":     contentToAnthropic(m.Content),
				}},
			})

		case protocol.CompactSummaryMessage:
			messages = append(messages, map[string]any{
				"role": "user", "content": "[Previous conversation summary]\n" + m.Summary,
			})
		}
	}
	body["messages"] = messages

	// Tools
	if len(ctx.Tools) > 0 {
		tools := make([]map[string]any, 0, len(ctx.Tools))
		for _, t := range ctx.Tools {
			tools = append(tools, map[string]any{
				"name": t.Name, "description": t.Description, "input_schema": t.Parameters,
			})
		}
		body["tools"] = tools
	}

	// Extended thinking
	if opts.Reasoning != "" && opts.Reasoning != protocol.ThinkingOff {
		budgetTokens := 10000
		if opts.ThinkingBudgets != nil {
			switch opts.Reasoning {
			case protocol.ThinkingLow:
				if v := opts.ThinkingBudgets.Low; v != nil { budgetTokens = *v }
			case protocol.ThinkingMedium:
				if v := opts.ThinkingBudgets.Medium; v != nil { budgetTokens = *v }
			case protocol.ThinkingHigh:
				if v := opts.ThinkingBudgets.High; v != nil { budgetTokens = *v }
			}
		}
		body["thinking"] = map[string]any{"type": "enabled", "budget_tokens": budgetTokens}
		if mt, ok := body["max_tokens"].(int); ok && mt <= budgetTokens {
			body["max_tokens"] = budgetTokens + 1024
		}
	}

	return body
}

// parseSSEStream reads the Anthropic SSE response and emits protocol events.
func (p *AnthropicProvider) parseSSEStream(ctx context.Context, body io.ReadCloser, model protocol.ModelDescriptor) (<-chan protocol.StreamEvent, error) {
	ch := make(chan protocol.StreamEvent, 64)

	var closeBody sync.Once
	doClose := func() { closeBody.Do(func() { body.Close() }) }

	go func() { <-ctx.Done(); doClose() }()

	go func() {
		defer doClose()
		defer close(ch)

		partial := &protocol.AssistantMessage{
			Role: "assistant", API: model.API, Provider: model.Provider,
			Model: model.ID, Timestamp: protocol.Now(),
		}

		snapshot := func() *protocol.AssistantMessage {
			s := *partial
			s.Content = make([]protocol.ContentBlock, len(partial.Content))
			copy(s.Content, partial.Content)
			return &s
		}

		ch <- protocol.StreamEvent{Type: protocol.EventStart, Partial: snapshot()}

		var blockIdx int
		var textOn, thinkOn bool
		var textBuf, thinkBuf, toolInputBuf strings.Builder
		var toolID, toolName string

		scanner := bufio.NewScanner(body)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				ch <- protocol.StreamEvent{Type: protocol.EventError, Reason: protocol.StopAborted, Message: snapshot()}
				return
			default:
			}

			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")

			var ev anthropicSSE
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				continue
			}

			switch ev.Type {
			case "message_start":
				if ev.Message != nil {
					if ev.Message.Usage != nil {
						partial.Usage.Input = ev.Message.Usage.InputTokens
					}
					partial.ResponseModel = ev.Message.Model
					partial.ResponseID = ev.Message.ID
				}

			case "content_block_start":
				blockIdx = ev.Index
				if ev.ContentBlock == nil {
					continue
				}
				switch ev.ContentBlock.Type {
				case "text":
					textOn = true
					textBuf.Reset()
					ch <- protocol.StreamEvent{Type: protocol.EventTextStart, ContentIndex: blockIdx, Partial: snapshot()}
				case "thinking":
					thinkOn = true
					thinkBuf.Reset()
					ch <- protocol.StreamEvent{Type: protocol.EventThinkingStart, ContentIndex: blockIdx, Partial: snapshot()}
				case "tool_use":
					toolID = ev.ContentBlock.ID
					toolName = ev.ContentBlock.Name
					toolInputBuf.Reset()
					ch <- protocol.StreamEvent{Type: protocol.EventToolCallStart, ContentIndex: blockIdx, Partial: snapshot()}
				}

			case "content_block_delta":
				if ev.Delta == nil {
					continue
				}
				switch ev.Delta.Type {
				case "text_delta":
					textBuf.WriteString(ev.Delta.Text)
					ch <- protocol.StreamEvent{Type: protocol.EventTextDelta, ContentIndex: blockIdx, Delta: ev.Delta.Text, Partial: snapshot()}
				case "thinking_delta":
					thinkBuf.WriteString(ev.Delta.Thinking)
					ch <- protocol.StreamEvent{Type: protocol.EventThinkingDelta, ContentIndex: blockIdx, Delta: ev.Delta.Thinking, Partial: snapshot()}
				case "input_json_delta":
					toolInputBuf.WriteString(ev.Delta.PartialJSON)
					ch <- protocol.StreamEvent{Type: protocol.EventToolCallDelta, ContentIndex: blockIdx, Delta: ev.Delta.PartialJSON, Partial: snapshot()}
				}

			case "content_block_stop":
				if textOn {
					partial.Content = append(partial.Content, protocol.TextContent{Type: "text", Text: textBuf.String()})
					ch <- protocol.StreamEvent{Type: protocol.EventTextEnd, ContentIndex: blockIdx, Content: textBuf.String(), Partial: snapshot()}
					textOn = false
				}
				if thinkOn {
					partial.Content = append(partial.Content, protocol.ThinkingContent{Type: "thinking", Thinking: thinkBuf.String()})
					ch <- protocol.StreamEvent{Type: protocol.EventThinkingEnd, ContentIndex: blockIdx, Content: thinkBuf.String(), Partial: snapshot()}
					thinkOn = false
				}
				if toolID != "" {
					tc := protocol.ToolCall{Type: "toolCall", ID: toolID, Name: toolName, Arguments: toolInputBuf.String()}
					partial.Content = append(partial.Content, tc)
					ch <- protocol.StreamEvent{Type: protocol.EventToolCallEnd, ContentIndex: blockIdx, ToolCall: &tc, Partial: snapshot()}
					toolID = ""
					toolName = ""
				}

			case "message_delta":
				if ev.Delta != nil && ev.Delta.StopReason != "" {
					partial.StopReason = mapAnthropicStopReason(ev.Delta.StopReason)
				}
				if ev.Usage != nil {
					partial.Usage.Output = ev.Usage.OutputTokens
					partial.Usage.TotalTokens = partial.Usage.Input + partial.Usage.Output
				}

			case "message_stop":
				if partial.StopReason == "" {
					partial.StopReason = protocol.StopEnd
				}
				ch <- protocol.StreamEvent{Type: protocol.EventDone, Reason: partial.StopReason, Message: snapshot(), Partial: snapshot()}
				return

			case "error":
				errMsg := "unknown Anthropic error"
				if ev.Error != nil {
					errMsg = ev.Error.Message
				}
				partial.ErrorMessage = errMsg
				partial.StopReason = protocol.StopError
				ch <- protocol.StreamEvent{Type: protocol.EventError, Reason: protocol.StopError, Message: snapshot()}
				return
			}
		}

		if ctx.Err() != nil {
			ch <- protocol.StreamEvent{Type: protocol.EventError, Reason: protocol.StopAborted, Message: snapshot()}
			return
		}
		if partial.StopReason == "" {
			partial.StopReason = protocol.StopEnd
		}
		ch <- protocol.StreamEvent{Type: protocol.EventDone, Reason: partial.StopReason, Message: snapshot(), Partial: snapshot()}
	}()

	return ch, nil
}

// --- Anthropic SSE JSON types ---

type anthropicSSE struct {
	Type         string                `json:"type"`
	Message      *anthropicMsg         `json:"message,omitempty"`
	Index        int                   `json:"index,omitempty"`
	ContentBlock *anthropicContentBlk  `json:"content_block,omitempty"`
	Delta        *anthropicDelta       `json:"delta,omitempty"`
	Usage        *anthropicUsage       `json:"usage,omitempty"`
	Error        *anthropicErr         `json:"error,omitempty"`
}

type anthropicMsg struct {
	ID    string           `json:"id"`
	Model string           `json:"model"`
	Usage *anthropicUsage  `json:"usage,omitempty"`
}

type anthropicContentBlk struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	ID       string          `json:"id,omitempty"`
	Name     string          `json:"name,omitempty"`
	Input    json.RawMessage `json:"input,omitempty"`
}

type anthropicDelta struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	Thinking    string `json:"thinking,omitempty"`
	PartialJSON string `json:"partial_json,omitempty"`
	StopReason  string `json:"stop_reason,omitempty"`
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type anthropicErr struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// --- Helpers ---

func resolveAnthropicKey(cred protocol.Credential) (string, error) {
	if cred == nil {
		return "", &protocol.Error{Code: protocol.ErrAuth, Message: "no credential provided"}
	}
	switch c := cred.(type) {
	case protocol.ApiKey:
		return c.Key, nil
	case protocol.BearerToken:
		return c.Token, nil
	default:
		return "", &protocol.Error{Code: protocol.ErrAuth, Message: fmt.Sprintf("unsupported credential type %T for anthropic provider", cred)}
	}
}

func contentToAnthropic(content []protocol.Content) any {
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
				"type":  "image",
				"source": map[string]any{
					"type":       "base64",
					"media_type": v.MimeType,
					"data":       v.Data,
				},
			})
		}
	}
	return parts
}

func mapAnthropicStopReason(reason string) protocol.StopReason {
	switch reason {
	case "end_turn":
		return protocol.StopEnd
	case "max_tokens":
		return protocol.StopLength
	case "tool_use":
		return protocol.StopTool
	case "stop_sequence":
		return protocol.StopEnd
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
var _ gateway.ProviderPort = (*AnthropicProvider)(nil)

// thinkingPtr is a helper to create a *ThinkingLevel from a value.
func thinkingPtr(level protocol.ThinkingLevel) *protocol.ThinkingLevel {
	return &level
}

// ---------------------------------------------------------------------------
// main — wire the custom provider through the gateway and agent
// ---------------------------------------------------------------------------

func main() {
	ctx := context.Background()

	// --- Step 1: Create the gateway ---
	gw := gateway.NewLLMGateway()

	// --- Step 2: Register the custom Anthropic provider ---
	// This is the key line: your custom ProviderPort plugs into the gateway
	// just like any built-in provider.
	gw.Providers.Register(NewAnthropicProvider(AnthropicConfig{}))

	// --- Step 3: Set up credentials ---
	gw.Credentials = gateway.CredentialProviderFunc(func(pid protocol.ProviderId) (protocol.Credential, error) {
		switch pid {
		case "anthropic":
			key := os.Getenv("ANTHROPIC_API_KEY")
			if key == "" {
				return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "ANTHROPIC_API_KEY not set"}
			}
			return protocol.ApiKey{Key: key}, nil
		default:
			return nil, &protocol.Error{Code: protocol.ErrAuth, Message: fmt.Sprintf("unknown provider: %s", pid)}
		}
	})

	// --- Step 4: Register the model in the catalog ---
	gw.Catalog.Register("anthropic", protocol.ModelDescriptor{
		ID:            "claude-sonnet-4-20250514",
		Name:          "Claude Sonnet 4",
		API:           protocol.ApiAnthropicMessages,
		Provider:      "anthropic",
		BaseURL:       "https://api.anthropic.com",
		ContextWindow: 200000,
		MaxTokens:     8192,
		Capabilities: protocol.ModelCapabilities{
			Input:     map[protocol.MediaType]bool{protocol.MediaText: true, protocol.MediaImage: true},
			Thinking:  true,
			ThinkingLevels: map[protocol.ThinkingLevel]bool{
				protocol.ThinkingOff:    true,
				protocol.ThinkingLow:    true,
				protocol.ThinkingMedium: true,
				protocol.ThinkingHigh:   true,
			},
			DefaultThinkingLevel: protocol.ThinkingLow, // Claude Sonnet 4 defaults to low
			Caching:   map[protocol.CacheMode]bool{protocol.CacheNone: true, protocol.CacheShort: true},
			Transport: map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
		Cost: protocol.CostRates{
			Input: 3.0, Output: 15.0, CacheRead: 0.3, CacheWrite: 3.75,
		},
	})

	model, ok := gw.Catalog.Get("anthropic", "claude-sonnet-4-20250514")
	if !ok {
		fmt.Fprintln(os.Stderr, "model not found")
		os.Exit(1)
	}

	// --- Step 5: Create the agent, wiring the gateway's Stream function ---
	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are a helpful assistant. Be concise.",
			// ThinkingLevel is nil (unset) → model's DefaultThinkingLevel ("low") will be used.
			// To explicitly disable thinking: ThinkingLevel: thinkingPtr(protocol.ThinkingOff)
			// To force high thinking:        ThinkingLevel: thinkingPtr(protocol.ThinkingHigh)
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	// --- Step 6: Subscribe to events ---
	ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.LoopEvent == nil {
			return
		}
		le := e.LoopEvent
		switch le.Type {
		case "agentStart":
			fmt.Println("[agent started]")
		case "messageUpdate":
			if le.StreamEvent != nil {
				switch le.StreamEvent.Type {
				case protocol.EventThinkingDelta:
					fmt.Printf("[thinking] %s", le.StreamEvent.Delta)
				case protocol.EventTextDelta:
					fmt.Print(le.StreamEvent.Delta)
				}
			}
		case "messageEnd":
			fmt.Println()
		case "agentEnd":
			fmt.Println("[agent ended]")
		}
	})

	// --- Step 7: Prompt the agent ---
	err := ag.Prompt(ctx, "Hello, who are you? Answer in one sentence.")
	if err != nil {
		fmt.Fprintf(os.Stderr, "prompt failed: %v\n", err)
		os.Exit(1)
	}

	// --- Step 8: Inspect the transcript ---
	state := ag.State()
	fmt.Printf("\nTranscript: %d messages\n", len(state.Messages))
	for i, msg := range state.Messages {
		role := protocol.MessageRole(msg)
		fmt.Printf("  [%d] %s", i, role)
		switch m := msg.(type) {
		case protocol.UserMessage:
			for _, c := range m.Content {
				if tc, ok := c.(protocol.TextContent); ok {
					fmt.Printf(": %q", tc.Text)
				}
			}
		case protocol.AssistantMessage:
			for _, c := range m.Content {
				switch b := c.(type) {
				case protocol.TextContent:
					fmt.Printf(": %q", b.Text)
				case protocol.ThinkingContent:
					fmt.Printf(": [thinking %d chars]", len(b.Thinking))
				}
			}
			fmt.Printf(" (tokens: in=%d out=%d)", m.Usage.Input, m.Usage.Output)
		}
		fmt.Println()
	}

	// --- Show cost ---
	if len(state.Messages) > 0 {
		if am, ok := state.Messages[len(state.Messages)-1].(protocol.AssistantMessage); ok {
			cost := gw.Catalog.CalculateCost(model, am.Usage)
			fmt.Printf("\nCost: $%.6f (input=$%.6f output=$%.6f)\n", cost.Total, cost.Input, cost.Output)
		}
	}
}