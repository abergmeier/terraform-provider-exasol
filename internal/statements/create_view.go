package statements

import (
	"context"
	"database/sql"
	"fmt"
)

type ViewColumn struct {
	Name    string
	Comment string
}

type CreateView struct {
	Schema   string
	Name     string
	Columns  []ViewColumn
	Subquery string
	Comment  string
	Replace  bool
}

// Execute creates or replaces View
func (s *CreateView) Execute(ctx context.Context, tx *sql.Tx) error {

	createPrefix := "CREATE VIEW"
	if s.Replace {
		createPrefix = "CREATE OR REPLACE VIEW"
	}

	viewComment := ""
	if s.Comment != "" {
		viewComment = fmt.Sprintf(" COMMENT IS '%s'", s.Comment)
	}

	var colPart string
	if s.Columns != nil {
		colPart = " ("
		for i, c := range s.Columns {
			if c.Comment == "" {
				colPart += c.Name
			} else {
				colPart += fmt.Sprintf("%s COMMENT IS '%s'", c.Name, c.Comment)
			}
			if i+1 != len(s.Columns) {
				colPart += ", "
			}
		}
		colPart += ")"
	}

	stmt := fmt.Sprintf("%s %s.%s%s AS %s%s", createPrefix, s.Schema, s.Name, colPart, s.Subquery, viewComment)
	_, err := tx.ExecContext(ctx, stmt)
	return err
}
