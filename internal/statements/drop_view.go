package statements

import (
	"fmt"

	"github.com/grantstreetgroup/go-exasol-client"
)

type DropView struct {
	Schema string
	Name   string
}

func (s *DropView) Execute(c *exasol.Conn) error {
	stmt := fmt.Sprintf("DROP VIEW %s", s.Name)
	_, err := c.Execute(stmt, nil, s.Schema)
	return err
}
