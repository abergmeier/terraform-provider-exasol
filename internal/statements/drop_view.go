package statements

import (
	"database/sql"
	"fmt"
)

type DropView struct {
	Schema string
	Name   string
}

func (s *DropView) Execute(tx *sql.Tx) error {
	stmt := fmt.Sprintf("DROP VIEW %s.%s", s.Schema, s.Name)
	_, err := tx.Exec(stmt)
	return err
}
