package protocol

// MessageRole returns the role string of a Message.
// This is a helper since the messageRole() method is defined on concrete types
// but callers outside the package can't easily access it through the interface.
func MessageRole(m Message) string {
	return m.(interface{ messageRole() string }).messageRole()
}
