module github.com/chinudotdev/pith/agent

go 1.24.5

require (
	github.com/chinudotdev/pith/gateway v0.0.0
	github.com/chinudotdev/pith/loop v0.0.0
	github.com/chinudotdev/pith/protocol v0.0.0
)

replace (
	github.com/chinudotdev/pith/gateway => ../gateway
	github.com/chinudotdev/pith/loop => ../loop
	github.com/chinudotdev/pith/protocol => ../protocol
)
