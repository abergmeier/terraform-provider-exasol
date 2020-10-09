package exaprovider

import (
	"fmt"

	"github.com/grantstreetgroup/go-exasol-client"
)

// Client implements everything that is needed to act as a Provider
// including the actual client to Exasol Websocket
type Client struct {
	conf exasol.ConnConf
}

type Locked struct {
	Conn *exasol.Conn
}

func NewClient(conf exasol.ConnConf) *Client {
	c := &Client{
		conf: conf,
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
		Conn: newConnect(c.conf),
	}
}

func (l *Locked) Unlock() {
	// Ensure that only explicitly committed operations stay
	conn := l.Conn
	l.Conn = nil
	err := conn.Rollback()
	if err != nil {
		fmt.Println("Rollback failed:", err)
	}
	conn.Disconnect()
}

func (c *Client) Close() error {
	return nil
}
