package exaprovider

import (
	"sync"

	"github.com/grantstreetgroup/go-exasol-client"
)

// Client implements everything that is needed to act as a Provider
// including the actual client to Exasol Websocket
type Client struct {
	conns [10]*exasol.Conn // For now we hardcode 10 parallel connections to Exasol
	i     int              // Only access index into conns with Mutex locked
	m     sync.Mutex
}

type locked struct {
	Conn *exasol.Conn
	m    *sync.Mutex
}

func NewClient(conf exasol.ConnConf) *Client {
	c := &Client{}

	for i := range c.conns {
		c.conns[i] = exasol.Connect(conf)
	}

	return c
}

func (c *Client) Lock() *locked {
	c.m.Lock()

	l := &locked{
		Conn: c.conns[c.i],
		m:    &c.m,
	}
	c.i = (c.i + 1) % len(c.conns)
	return l
}

func (l *locked) Unlock() {
	l.m.Unlock()
}

func (c *Client) Execute(sql string, args ...interface{}) (map[string]interface{}, error) {
	locked := c.Lock()
	defer locked.Unlock()

	return locked.Conn.Execute(sql, args...)
}

func (c *Client) FetchSlice(sql string, args ...interface{}) ([][]interface{}, error) {
	locked := c.Lock()
	defer locked.Unlock()

	return locked.Conn.FetchSlice(sql, args...)
}

func (c *Client) Close() error {
	c.m.Lock()

	for i := range c.conns {
		c.conns[i].Disconnect()
		c.conns[i] = nil
	}

	c.m.Unlock()
	return nil
}
