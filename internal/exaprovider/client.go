package exaprovider

import (
	"github.com/grantstreetgroup/go-exasol-client"
)

// Client implements everything that is needed to act as a Provider
// including the actual client to Exasol Websocket
type Client struct {
	Conn *exasol.Conn
}
