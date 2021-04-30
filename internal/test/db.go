package test

import (
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
)

func Commit(t *testing.T, c internal.Conn) {
	err := c.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func Execute(t *testing.T, c internal.Conn, stmt string) (rowsAffected int64) {
	var err error
	rowsAffected, err = c.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}
	return rowsAffected
}
