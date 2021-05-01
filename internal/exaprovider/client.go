package exaprovider

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/abergmeier/terraform-provider-exasol/internal/websocket"
	"github.com/grantstreetgroup/go-exasol-client"
)

// Client implements everything that is needed to act as a Provider
// including the actual client to Exasol Websocket
type Client struct {
	conf            exasol.ConnConf
	cacheBucketName string
}

type Connection struct {
	Conn *exasol.Conn
	WS   *websocket.JSONCache
}

func NewClient(conf exasol.ConnConf, cacheBucketName string) (*Client, error) {
	c := &Client{
		conf:            conf,
		cacheBucketName: cacheBucketName,
	}

	return c, nil
}

func newConnect(conf exasol.ConnConf, ws *websocket.JSONCache) (*exasol.Conn, error) {
	var err error
	var conn *exasol.Conn
	if ws == nil {
		conn, err = exasol.Connect(conf)
	} else {
		conn, err = exasol.WrapConnectedWebSocket(conf, ws)
	}
	if err != nil {
		return nil, err
	}
	conn.DisableAutoCommit()
	return conn, nil
}

func (c *Client) OpenManualConnection() (*Connection, error) {
	var ws *websocket.JSONCache
	if c.cacheBucketName != "" {
		ctx := context.Background()
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
		bucket := client.Bucket(c.cacheBucketName)
		ws = &websocket.JSONCache{
			Storage: bucket,
		}
	}
	conn, err := newConnect(c.conf, ws)
	if err != nil {
		return nil, err
	}
	return &Connection{
		Conn: conn,
	}, nil
}

func (c *Connection) Close() error {
	// Ensure that only explicitly committed operations stay
	conn := c.Conn
	c.Conn = nil
	err := conn.Rollback()
	if err != nil {
		fmt.Println("Rollback failed:", err)
	}
	conn.Disconnect()
	return nil
}
