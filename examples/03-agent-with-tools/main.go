// Example 03: Agent with Tools — define tools, the model calls them, the agent executes them.
//
//	mkdir my-agent && cd my-agent && go mod init my-agent
//	cp main.go . && go mod tidy
//	OPENAI_API_KEY="sk-..." go run main.go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/chinudotdev/pith/agent"
	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/gateway/providers"
	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

func main() {
	ctx := context.Background()

	gw := gateway.NewLLMGateway()
	gw.Providers.Register(providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
		BaseURL: "https://api.openai.com",
	}))
	gw.Credentials = gateway.CredentialProviderFunc(func(pid protocol.ProviderId) (protocol.Credential, error) {
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "OPENAI_API_KEY not set"}
		}
		return protocol.ApiKey{Key: key}, nil
	})
	gw.Catalog.Register("openai", protocol.ModelDescriptor{
		ID:            "gpt-4o-mini",
		Name:          "GPT-4o Mini",
		API:           protocol.ApiOpenAICompletions,
		Provider:      "openai",
		BaseURL:       "https://api.openai.com",
		ContextWindow: 128000,
		MaxTokens:     4096,
		Capabilities: protocol.ModelCapabilities{
			Input:     map[protocol.MediaType]bool{protocol.MediaText: true},
			Transport: map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
	})

	model, _ := gw.Catalog.Get("openai", "gpt-4o-mini")

	// Define tools.
	readFileTool := loop.AgentTool{
		Name:        "read_file",
		Label:       "Read File",
		Description: "Read the contents of a file from disk",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{"type": "string", "description": "The path to the file"},
			},
			"required": []string{"path"},
		},
		Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
			path, _ := params["path"].(string)
			fmt.Printf("  [tool] read_file: path=%q\n", path)
			content := fmt.Sprintf("Contents of %s:\nHello from the file!\n", path)
			return loop.ToolResult{
				Content: []protocol.Content{protocol.TextContent{Type: "text", Text: content}},
			}
		},
	}

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are a helpful coding assistant. Use tools when asked about files.",
			Tools:        []loop.AgentTool{readFileTool},
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	// Subscribe to tool execution events.
	ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.LoopEvent == nil {
			return
		}
		le := e.LoopEvent
		switch le.Type {
		case loop.LoopToolExecutionStart:
			fmt.Printf("  [event] tool started: %s\n", le.ToolName)
		case loop.LoopToolExecutionEnd:
			fmt.Printf("  [event] tool ended: %s\n", le.ToolName)
		case "messageUpdate":
			if le.StreamEvent != nil && le.StreamEvent.Type == protocol.EventTextDelta {
				fmt.Print(le.StreamEvent.Delta)
			}
		case "messageEnd":
			fmt.Println()
		}
	})

	err := ag.Prompt(ctx, "Read the file /tmp/hello.txt and tell me what's in it.")
	if err != nil {
		fmt.Fprintf(os.Stderr, "prompt failed: %v\n", err)
		os.Exit(1)
	}

	// Show the transcript.
	fmt.Println("\n--- Transcript ---")
	state := ag.State()
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
				case protocol.ToolCall:
					fmt.Printf(": [toolCall %s(%s)]", b.Name, b.Arguments)
				}
			}
		case protocol.ToolResultMessage:
			fmt.Printf(": toolCallId=%s", m.ToolCallID)
		}
		fmt.Println()
	}
}
