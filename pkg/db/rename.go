package db

import (
	"fmt"

	"github.com/grantstreetgroup/go-exasol-client"
)

// Rename changes the name on the Database
func Rename(c *exasol.Conn, t, old, new, schema string) error {

	stmt := fmt.Sprintf("RENAME %s %s TO %s", t, old, new)
	var err error
	if schema == "" {
		_, err = c.Execute(stmt)
	} else {
		_, err = c.Execute(stmt, nil, schema)
	}
	return err
}
