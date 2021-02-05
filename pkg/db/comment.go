package db

import (
	"fmt"

	"github.com/grantstreetgroup/go-exasol-client"
)

// Comment changes the comment on the Database object
func Comment(c *exasol.Conn, t, objectName, newComment, schema string) error {

	stmt := fmt.Sprintf("COMMENT ON %s %s IS %s", t, objectName, newComment)
	_, err := c.Execute(stmt, nil, schema)
	return err
}
