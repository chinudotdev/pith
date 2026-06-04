package protocol

// Identifiers are string-typed for flexibility.
// Known values are defined as constants below.

type ApiId = string    // e.g. "openai-completions", "anthropic-messages"
type ProviderId = string // e.g. "anthropic", "openai", "google"

// Known API identifiers.
const (
	ApiOpenAICompletions   ApiId = "openai-completions"
	ApiOpenAIResponses     ApiId = "openai-responses"
	ApiAzureOpenAI         ApiId = "azure-openai-responses"
	ApiOpenAICodex         ApiId = "openai-codex-responses"
	ApiAnthropicMessages   ApiId = "anthropic-messages"
	ApiBedrockConverse     ApiId = "bedrock-converse-stream"
	ApiGoogleGenAI         ApiId = "google-generative-ai"
	ApiGoogleVertex        ApiId = "google-vertex"
	ApiMistralConversations ApiId = "mistral-conversations"
)

// Known provider identifiers.
const (
	ProviderAnthropic    ProviderId = "anthropic"
	ProviderOpenAI       ProviderId = "openai"
	ProviderGoogle       ProviderId = "google"
	ProviderGoogleVertex ProviderId = "google-vertex"
	ProviderAmazonBedrock ProviderId = "amazon-bedrock"
	ProviderMistral      ProviderId = "mistral"
	ProviderDeepSeek     ProviderId = "deepseek"
	ProviderGroq         ProviderId = "groq"
	ProviderOpenRouter   ProviderId = "openrouter"
	ProviderFireworks    ProviderId = "fireworks"
	ProviderTogether     ProviderId = "together"
	ProviderXAI          ProviderId = "xai"
	ProviderCerebras     ProviderId = "cerebras"
	ProviderGitHubCopilot ProviderId = "github-copilot"
)
