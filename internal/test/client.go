package test

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
)

func OpenManualConnection(c *exaprovider.Client) *exaprovider.Connection {
	conn, err := c.OpenManualConnection()
	if err != nil {
		panic(fmt.Sprintf("Manual Connection failed: %s", err))
	}
	return conn
}

func OpenManualConnectionInTest(t *testing.T, c *exaprovider.Client) *exaprovider.Connection {
	conn, err := c.OpenManualConnection()
	if err != nil {
		t.Fatal("Manual Connection failed:", err)
	}
	return conn
}
