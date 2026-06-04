package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/protocol"
)

// newTestProvider creates an OpenAICompatProvider with sensible test defaults.
func newTestProvider() *OpenAICompatProvider {
	return NewOpenAICompatProvider(OpenAICompatConfig{
		BaseURL: "https://api.openai.com",
	})
}

// --- buildRequest tests ---

func TestBuildRequest_UsageStreamOptions(t *testing.T) {
	// Issue #1 regression: when SupportsUsageInStreaming is true,
	// the request body must include stream_options.include_usage.
	p := NewOpenAICompatProvider(OpenAICompatConfig{
		BaseURL: "https://api.openai.com",
		Capabilities: gateway.ProviderCapabilities{
			SupportsUsageInStreaming: true,
			MaxTokensField:          gateway.MaxTokensCompletion,
		},
	})

	body := p.buildRequest(
		protocol.ModelDescriptor{ID: "gpt-4o"},
		protocol.Context{SystemPrompt: "hi", Messages: nil},
		protocol.StreamOptions{},
	)

	// Must have stream_options.include_usage = true
	streamOpts, ok := body["stream_options"].(map[string]any)
	if !ok {
		t.Fatal("expected stream_options in request body when SupportsUsageInStreaming is true")
	}
	if streamOpts["include_usage"] != true {
		t.Errorf("expected include_usage=true, got %v", streamOpts["include_usage"])
	}
}

func TestBuildRequest_NoStreamOptionsWithoutCapability(t *testing.T) {
	p := NewOpenAICompatProvider(OpenAICompatConfig{
		BaseURL: "https://api.openai.com",
		Capabilities: gateway.ProviderCapabilities{
			SupportsUsageInStreaming: false,
			MaxTokensField:          gateway.MaxTokensLegacy, // non-zero to avoid auto-defaults
		},
	})

	body := p.buildRequest(
		protocol.ModelDescriptor{ID: "gpt-4o"},
		protocol.Context{SystemPrompt: "hi"},
		protocol.StreamOptions{},
	)

	if _, exists := body["stream_options"]; exists {
		t.Error("stream_options should not be present when SupportsUsageInStreaming is false")
	}
}

func TestBuildRequest_AssistantMessageWithToolCalls(t *testing.T) {
	// Issue #2 regression: ToolCall blocks must be serialized as top-level
	// tool_calls array, NOT inside the content array.
	p := newTestProvider()

	msgs := []protocol.Message{
		protocol.UserMessage{
			Role: "user",
			Content: []protocol.Content{
				protocol.TextContent{Type: "text", Text: "use the tool"},
			},
		},
		protocol.AssistantMessage{
			Role: "assistant",
			Content: []protocol.ContentBlock{
				protocol.TextContent{Type: "text", Text: "calling tool now"},
				protocol.ToolCall{
					Type:      "toolCall",
					ID:        "call_abc123",
					Name:      "get_weather",
					Arguments: `{"city":"SF"}`,
				},
			},
		},
		protocol.ToolResultMessage{
			Role:       "toolResult",
			ToolCallID: "call_abc123",
			ToolName:   "get_weather",
			Content: []protocol.Content{
				protocol.TextContent{Type: "text", Text: "72°F sunny"},
			},
		},
	}

	body := p.buildRequest(
		protocol.ModelDescriptor{ID: "gpt-4o"},
		protocol.Context{Messages: msgs},
		protocol.StreamOptions{},
	)

	raw, _ := json.Marshal(body)
	t.Logf("Request body: %s", string(raw))

	requestMsgs, ok := body["messages"].([]map[string]any)
	if !ok {
		t.Fatalf("expected messages to be []map[string]any, got %T", body["messages"])
	}

	// Find the assistant message
	var assistantMsg map[string]any
	for _, m := range requestMsgs {
		if m["role"] == "assistant" {
			assistantMsg = m
			break
		}
	}
	if assistantMsg == nil {
		t.Fatal("no assistant message found in request")
	}

	// Tool calls must be in top-level "tool_calls" array
	toolCalls, hasToolCalls := assistantMsg["tool_calls"].([]map[string]any)
	if !hasToolCalls || len(toolCalls) == 0 {
		t.Error("assistant message with ToolCall blocks must have top-level tool_calls array")
	} else {
		tc := toolCalls[0]
		if tc["id"] != "call_abc123" {
			t.Errorf("expected tool call id 'call_abc123', got %v", tc["id"])
		}
		fn, ok := tc["function"].(map[string]any)
		if !ok {
			t.Errorf("expected tool call to have 'function' map, got %T", tc["function"])
		} else {
			if fn["name"] != "get_weather" {
				t.Errorf("expected function name 'get_weather', got %v", fn["name"])
			}
			if fn["arguments"] != `{"city":"SF"}` {
				t.Errorf("expected function arguments '{\"city\":\"SF\"}', got %v", fn["arguments"])
			}
		}
	}

	// Content must NOT contain tool_call type entries
	content, hasContent := assistantMsg["content"]
	if hasContent {
		switch c := content.(type) {
		case string:
			// plain string content is fine
		case []map[string]any:
			for _, block := range c {
				if block["type"] == "tool_call" {
					t.Error("content array must NOT contain type='tool_call' — tool calls go in top-level tool_calls array")
				}
			}
		}
	}
}

func TestBuildRequest_AssistantMessageTextOnly(t *testing.T) {
	p := newTestProvider()

	msgs := []protocol.Message{
		protocol.AssistantMessage{
			Role: "assistant",
			Content: []protocol.ContentBlock{
				protocol.TextContent{Type: "text", Text: "just text, no tools"},
			},
		},
	}

	body := p.buildRequest(
		protocol.ModelDescriptor{ID: "gpt-4o"},
		protocol.Context{Messages: msgs},
		protocol.StreamOptions{},
	)

	requestMsgs := body["messages"].([]map[string]any)
	assistantMsg := requestMsgs[0]

	// Text-only assistant: content should be a plain string
	if content, ok := assistantMsg["content"].(string); !ok || content != "just text, no tools" {
		t.Errorf("expected plain string content, got %v", assistantMsg["content"])
	}

	// No tool_calls key
	if _, hasToolCalls := assistantMsg["tool_calls"]; hasToolCalls {
		t.Error("text-only assistant message should not have tool_calls key")
	}
}

func TestBuildRequest_ToolResultMessage(t *testing.T) {
	p := newTestProvider()

	msgs := []protocol.Message{
		protocol.ToolResultMessage{
			Role:       "toolResult",
			ToolCallID: "call_xyz",
			ToolName:   "read_file",
			Content: []protocol.Content{
				protocol.TextContent{Type: "text", Text: "file contents"},
			},
		},
	}

	body := p.buildRequest(
		protocol.ModelDescriptor{ID: "gpt-4o"},
		protocol.Context{Messages: msgs},
		protocol.StreamOptions{},
	)

	requestMsgs := body["messages"].([]map[string]any)
	toolMsg := requestMsgs[0]

	if toolMsg["role"] != "tool" {
		t.Errorf("expected role='tool', got %v", toolMsg["role"])
	}
	if toolMsg["tool_call_id"] != "call_xyz" {
		t.Errorf("expected tool_call_id='call_xyz', got %v", toolMsg["tool_call_id"])
	}
}

// --- parseSSEStream tests ---

func TestParseSSEStream_UsageInFinalChunk(t *testing.T) {
	// Issue #1 regression: usage data from a chunk with empty choices
	// must be captured (OpenAI sends usage in a separate chunk after
	// stream_options.include_usage=true).
	sseData := strings.Join([]string{
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}]}`,
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":25,"completion_tokens":3,"total_tokens":28}}`,
		`data: [DONE]`,
		"", // trailing newline
	}, "\n")

	body := io.NopCloser(strings.NewReader(sseData))
	p := newTestProvider()
	model := protocol.ModelDescriptor{ID: "gpt-4o", API: protocol.ApiOpenAICompletions, Provider: "openai"}

	ch, err := p.parseSSEStream(context.Background(), body, model)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var gotUsage *protocol.Usage
	for event := range ch {
		if event.Type == protocol.EventDone && event.Message != nil {
			u := event.Message.Usage
			gotUsage = &u
		}
	}

	if gotUsage == nil {
		t.Fatal("expected usage in EventDone message, got nil")
	}
	if gotUsage.Input != 25 {
		t.Errorf("expected usage.Input=25, got %d", gotUsage.Input)
	}
	if gotUsage.Output != 3 {
		t.Errorf("expected usage.Output=3, got %d", gotUsage.Output)
	}
}

func TestParseSSEStream_ToolCalls(t *testing.T) {
	sseData := strings.Join([]string{
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":null,"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}`,
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]},"finish_reason":null}]}`,
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"SF\"}"}}]},"finish_reason":null}]}`,
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		`data: [DONE]`,
		"",
	}, "\n")

	body := io.NopCloser(strings.NewReader(sseData))
	p := newTestProvider()
	model := protocol.ModelDescriptor{ID: "gpt-4o", API: protocol.ApiOpenAICompletions, Provider: "openai"}

	ch, err := p.parseSSEStream(context.Background(), body, model)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var toolCallEnds []protocol.ToolCall
	var stopReason protocol.StopReason
	for event := range ch {
		if event.Type == protocol.EventToolCallEnd && event.ToolCall != nil {
			toolCallEnds = append(toolCallEnds, *event.ToolCall)
		}
		if event.Type == protocol.EventDone {
			stopReason = event.Reason
		}
	}

	if len(toolCallEnds) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(toolCallEnds))
	}
	tc := toolCallEnds[0]
	if tc.ID != "call_1" {
		t.Errorf("expected tool call id 'call_1', got %s", tc.ID)
	}
	if tc.Name != "get_weather" {
		t.Errorf("expected tool call name 'get_weather', got %s", tc.Name)
	}
	if tc.Arguments != `{"city":"SF"}` {
		t.Errorf("expected arguments '{\"city\":\"SF\"}', got %s", tc.Arguments)
	}
	if stopReason != protocol.StopTool {
		t.Errorf("expected stopReason=tool, got %s", stopReason)
	}
}

func TestParseSSEStream_UsageBeforeChoicesContinue(t *testing.T) {
	// Verify that a chunk with usage AND choices still works
	// (some providers send usage alongside the last choice).
	sseData := strings.Join([]string{
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`,
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":1,"total_tokens":11}}`,
		`data: [DONE]`,
		"",
	}, "\n")

	body := io.NopCloser(strings.NewReader(sseData))
	p := newTestProvider()
	model := protocol.ModelDescriptor{ID: "gpt-4o", API: protocol.ApiOpenAICompletions, Provider: "openai"}

	ch, err := p.parseSSEStream(context.Background(), body, model)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var gotUsage *protocol.Usage
	for event := range ch {
		if event.Type == protocol.EventDone && event.Message != nil {
			u := event.Message.Usage
			gotUsage = &u
		}
	}

	if gotUsage == nil {
		t.Fatal("expected usage in EventDone message")
	}
	if gotUsage.Input != 10 {
		t.Errorf("expected usage.Input=10, got %d", gotUsage.Input)
	}
	if gotUsage.Output != 1 {
		t.Errorf("expected usage.Output=1, got %d", gotUsage.Output)
	}
}

func TestBuildRequest_CompactSummaryMessage(t *testing.T) {
	p := newTestProvider()

	msgs := []protocol.Message{
		protocol.CompactSummaryMessage{
			Role:      "compactSummary",
			Summary:   "Previous discussion about Go",
			Timestamp: protocol.Now(),
		},
	}

	body := p.buildRequest(
		protocol.ModelDescriptor{ID: "gpt-4o"},
		protocol.Context{Messages: msgs},
		protocol.StreamOptions{},
	)

	requestMsgs := body["messages"].([]map[string]any)
	csmMsg := requestMsgs[0]

	if csmMsg["role"] != "user" {
		t.Errorf("CompactSummary should be converted to role='user', got %v", csmMsg["role"])
	}
	content, ok := csmMsg["content"].(string)
	if !ok || !strings.Contains(content, "Previous discussion about Go") {
		t.Errorf("expected content to contain summary text, got %v", csmMsg["content"])
	}
}

// --- Context cancellation (Issue #4) tests ---

func TestStream_ContextCancellation(t *testing.T) {
	// Issue #4 regression: cancelling the context must abort the in-flight HTTP request.
	// We set up a slow server and cancel the context mid-request.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a slow streaming response
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("response writer does not support flushing")
		}
		// Send a few events slowly
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n")
			flusher.Flush()
			time.Sleep(100 * time.Millisecond)
		}
	}))
	defer server.Close()

	p := NewOpenAICompatProvider(OpenAICompatConfig{
		BaseURL: server.URL,
		Capabilities: gateway.ProviderCapabilities{
			MaxTokensField: gateway.MaxTokensCompletion,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	model := protocol.ModelDescriptor{
		ID:       "test-model",
		API:      protocol.ApiOpenAICompletions,
		Provider: "test",
	}

	ch, err := p.Stream(ctx, model, protocol.Context{SystemPrompt: "hi"}, protocol.StreamOptions{
		Credential: protocol.ApiKey{Key: "test-key"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Read events from the channel — context should cancel mid-stream
	var gotError bool
	for event := range ch {
		if event.Type == protocol.EventError {
			gotError = true
			if event.Reason != protocol.StopAborted {
				t.Errorf("expected StopAborted reason, got %s", event.Reason)
			}
		}
	}

	if !gotError {
		t.Error("expected EventError from cancelled context during streaming")
	}
}

func TestStream_ContextPassedToHTTPRequest(t *testing.T) {
	// Verify that context cancellation actually cancels the HTTP request.
	// We set up a server that stalls after sending headers, then cancel
	// the context and verify the stream ends promptly.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		flusher.Flush()
		// Stall indefinitely — the client should disconnect on cancel
		<-r.Context().Done()
	}))
	defer server.Close()

	p := NewOpenAICompatProvider(OpenAICompatConfig{
		BaseURL: server.URL,
		Capabilities: gateway.ProviderCapabilities{
			MaxTokensField: gateway.MaxTokensCompletion,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())

	model := protocol.ModelDescriptor{
		ID:       "test-model",
		API:      protocol.ApiOpenAICompletions,
		Provider: "test",
	}

	ch, err := p.Stream(ctx, model, protocol.Context{SystemPrompt: "hi"}, protocol.StreamOptions{
		Credential: protocol.ApiKey{Key: "test-key"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Cancel after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// The channel should close within a reasonable time after cancel
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				// Channel closed — context cancellation worked
				return
			}
		case <-deadline:
			t.Fatal("stream did not close within 2s after context cancellation")
		}
	}
}
