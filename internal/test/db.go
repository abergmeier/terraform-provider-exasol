package test

import (
	"database/sql"
	"testing"
)

func Commit(t *testing.T, tx *sql.Tx) {
	err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func Execute(t *testing.T, tx *sql.Tx, stmt string) (rowsAffected int64) {
	r, err := tx.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}
	rowsAffected, err = r.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	return
}
