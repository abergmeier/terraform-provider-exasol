package db

import "github.com/grantstreetgroup/go-exasol-client"

// MustCommit tries commit and fails hard if it does not work
func MustCommit(c *exasol.Conn) {
	err := c.Commit()
	if err != nil {
		panic(err)
	}
}
