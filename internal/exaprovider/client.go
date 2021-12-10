package exaprovider

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"database/sql"

	"github.com/exasol/exasol-driver-go"
)

// Client implements everything that is needed to act as a Provider
// including the actual client to Exasol Websocket
type Client struct {
	conf *exasol.DSNConfig
}

type Locked struct {
	Conf *exasol.DSNConfig
	Tx   *sql.Tx
}

func NewClient(conf *exasol.DSNConfig) *Client {
	c := &Client{
		conf: conf,
	}

	return c
}

func (c *Client) Lock(ctx context.Context) *Locked {
	db, err := sql.Open("exasol", c.conf.Autocommit(false).String())
	if err != nil {
		panic(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	return &Locked{
		Conf: c.conf,
		Tx:   tx,
	}
}

func (l *Locked) Unlock() {
	// Ensure that only explicitly committed operations stay
	err := l.Tx.Rollback()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		fmt.Println("Rollback failed:", err)
	}

	l.Tx = nil
}

func TestLock(t *testing.T, c *Client) *Locked {
	return c.Lock(context.TODO())
}
