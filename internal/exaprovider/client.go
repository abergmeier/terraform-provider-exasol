package exaprovider

import (
	"github.com/grantstreetgroup/go-exasol-client"
)

// Client implements everything that is needed to act as a Provider
// including the actual client to Exasol Websocket
type Client struct {
	conns chan *exasol.Conn
}

type Locked struct {
	Conn  *exasol.Conn
	conns *chan *exasol.Conn
}

func NewClient(conf exasol.ConnConf) *Client {
	c := &Client{
		conns: make(chan *exasol.Conn, 10), // For now we hardcode 10 parallel connections to Exasol
	}

	for i := 0; i != cap(c.conns); i++ {
		conn := exasol.Connect(conf)
		conn.DisableAutoCommit()
		c.conns <- conn
	}

	return c
}

func (c *Client) Lock() *Locked {
	return &Locked{
		Conn:  <-c.conns,
		conns: &c.conns,
	}
}

func (l *Locked) Unlock() {
	// Ensure that only explicitly committed operations stay
	l.Conn.Rollback()
	*l.conns <- l.Conn
	l.Conn = nil
}

func (c *Client) Close() error {
	for i := 0; i != cap(c.conns); i++ {
		conn := <-c.conns
		conn.Disconnect()
	}

	return nil
}
