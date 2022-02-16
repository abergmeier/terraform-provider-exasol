package exaprovider

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
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
	if conf.ClientName == "" {
		conf.ClientName = "Terraform"
	}
	if conf.ClientVersion == "" {
		info, _ := debug.ReadBuildInfo()
		if info != nil {
			conf.ClientVersion = info.Main.Version
			/*
				info.["gitrevision"]
			*/
		}
	}
	c := &Client{
		conf: conf,
	}

	return c
}

func (c *Client) Lock(ctx context.Context) *Locked {
	// All internal logic is based on transactions and
	// rolling back changes when these fail so disable
	// autocommit
	*c.conf.Autocommit = false
	db, err := sql.Open("exasol", c.conf.ToDSN())
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
