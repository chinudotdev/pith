package protocol

// --- Credential Types ---

// Credential is the resolved authentication credential for a provider.
// The SDK defines the interface; the App provides implementations.
type Credential interface {
	credentialType() string
}

// ApiKey is a simple API key credential.
type ApiKey struct {
	Key string `json:"key"`
}

func (ApiKey) credentialType() string { return "apiKey" }

// BearerToken is a bearer token with optional refresh.
type BearerToken struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expiresAt,omitempty"` // Unix seconds, 0 = never expires
}

func (BearerToken) credentialType() string { return "bearerToken" }

// AwsCredentials represents AWS credentials for Bedrock.
type AwsCredentials struct {
	Profile        string `json:"profile,omitempty"`
	Region         string `json:"region"`
	AccessKeyID    string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	BearerToken    string `json:"bearerToken,omitempty"`
}

func (AwsCredentials) credentialType() string { return "awsCredentials" }

// GcpCredentials represents GCP credentials for Vertex AI.
type GcpCredentials struct {
	Project  string `json:"project"`
	Location string `json:"location"`
	ADC      bool   `json:"adc"` // use Application Default Credentials
}

func (GcpCredentials) credentialType() string { return "gcpCredentials" }

// OAuthToken represents an OAuth2 token.
type OAuthToken struct {
	Token        string   `json:"token"`
	RefreshToken string   `json:"refreshToken"`
	ExpiresAt    int64    `json:"expiresAt"` // Unix seconds
	Scopes       []string `json:"scopes"`
}

func (OAuthToken) credentialType() string { return "oauthToken" }
