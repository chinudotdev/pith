package protocol

import "time"

// --- Content Blocks ---

// TextContent represents a text block in any message.
type TextContent struct {
	Type          string `json:"type"`                     // always "text"
	Text          string `json:"text"`
	TextSignature string `json:"textSignature,omitempty"` // Anthropic signature
}

// ImageContent represents an image block.
type ImageContent struct {
	Type     string `json:"type"` // always "image"
	Data     string `json:"data"` // base64-encoded
	MimeType string `json:"mimeType"`
}

// Content is the union of content types that can appear in user messages.
type Content interface {
	contentType()
}

func (TextContent) contentType()  {}
func (ImageContent) contentType() {}

// ThinkingContent represents a thinking/reasoning block from the model.
type ThinkingContent struct {
	Type              string `json:"type"` // always "thinking"
	Thinking          string `json:"thinking"`
	ThinkingSignature string `json:"thinkingSignature,omitempty"`
	Redacted          bool   `json:"redacted,omitempty"`
}

// ToolCall represents a tool invocation request from the model.
type ToolCall struct {
	Type            string `json:"type"` // always "toolCall"
	ID              string `json:"id"`
	Name            string `json:"name"`
	Arguments       string `json:"arguments"` // JSON string
	ThoughtSignature string `json:"thoughtSignature,omitempty"`
}

// ContentBlock is the union of content types that can appear in assistant messages.
type ContentBlock interface {
	contentBlockType()
}

func (TextContent) contentBlockType()     {}
func (ImageContent) contentBlockType()    {}
func (ThinkingContent) contentBlockType() {}
func (ToolCall) contentBlockType()        {}

// --- Messages ---

// UserMessage is a message from the user.
type UserMessage struct {
	Role      string    `json:"role"` // always "user"
	Content   []Content `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// AssistantMessage is a message from the model.
type AssistantMessage struct {
	Role          string         `json:"role"` // always "assistant"
	Content       []ContentBlock `json:"content"`
	Usage         Usage          `json:"usage"`
	StopReason    StopReason     `json:"stopReason"`
	ErrorMessage  string         `json:"errorMessage,omitempty"`
	API           ApiId          `json:"api"`
	Provider      ProviderId     `json:"provider"`
	Model         string         `json:"model"`
	ResponseModel string         `json:"responseModel,omitempty"`
	ResponseID    string         `json:"responseId,omitempty"`
	Diagnostics   []Diagnostic   `json:"diagnostics,omitempty"`
	Timestamp     time.Time      `json:"timestamp"`
}

// ToolResultMessage is the result of executing a tool call.
type ToolResultMessage struct {
	Role       string    `json:"role"` // always "toolResult"
	ToolCallID string    `json:"toolCallId"`
	ToolName   string    `json:"toolName"`
	Content    []Content `json:"content"`
	Details    any       `json:"details,omitempty"`
	IsError    bool      `json:"isError"`
	Timestamp  time.Time `json:"timestamp"`
}

// CompactSummaryMessage replaces old messages after compaction.
type CompactSummaryMessage struct {
	Role         string        `json:"role"` // always "compactSummary"
	Summary      string        `json:"summary"`
	TokensBefore int           `json:"tokensBefore"`
	TokensAfter  int           `json:"tokensAfter"`
	FileOps      *FileOperations `json:"fileOps,omitempty"`
	Timestamp    time.Time     `json:"timestamp"`
}

// FileOperations tracks which files were read/written/edited during a conversation.
type FileOperations struct {
	Read    []string `json:"read"`
	Written []string `json:"written"`
	Edited  []string `json:"edited"`
}

// Message is the union of all message types.
type Message interface {
	messageRole() string
}

func (UserMessage) messageRole() string          { return "user" }
func (AssistantMessage) messageRole() string     { return "assistant" }
func (ToolResultMessage) messageRole() string    { return "toolResult" }
func (CompactSummaryMessage) messageRole() string { return "compactSummary" }
