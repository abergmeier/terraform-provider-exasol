package tx

import (
	"database/sql"
	"fmt"
)

func MustExec(tx *sql.Tx, stmt string, args ...interface{}) {
	_, err := tx.Exec(stmt, args...)
	if err != nil {
		panic(err)
	}
}

func MustExecf(tx *sql.Tx, format string, args ...interface{}) {
	MustExec(tx, fmt.Sprintf(format, args...))
}

func MustQuery(tx *sql.Tx, stmt string, args ...interface{}) *sql.Rows {
	r, err := tx.Query(stmt, args...)
	if err != nil {
		panic(err)
	}
	return r
}

// MustQueryAtLeastOne ensures that the Query executed and that calls Next
// If any operation fails it immediately panics
func MustQueryAtLeastOne(tx *sql.Tx, stmt string, args ...interface{}) *sql.Rows {
	r := MustQuery(tx, stmt, args...)
	if !r.Next() {
		panic(fmt.Sprintf("No result found for: %s", stmt))
	}
	return r
}
