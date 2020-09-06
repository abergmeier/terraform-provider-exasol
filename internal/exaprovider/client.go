package exaprovider

import (
	"fmt"

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
		c.conns <- newConnect(conf)
	}

	return c
}

func newConnect(conf exasol.ConnConf) *exasol.Conn {
	conn := exasol.Connect(conf)
	conn.DisableAutoCommit()
	return conn
}

func (c *Client) Lock() *Locked {
	return &Locked{
		Conn:  <-c.conns,
		conns: &c.conns,
	}
}

func (l *Locked) Unlock() {
	// Ensure that only explicitly committed operations stay
	err := l.Conn.Rollback()
	if err != nil {
		fmt.Println("Rollback failed:", err)

	}
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
