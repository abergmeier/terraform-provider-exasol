package db

import (
	"database/sql"
	"fmt"
)

// Rename changes the name on the Database
func Rename(tx *sql.Tx, t, old, new, schema string) error {

	var err error
	var stmt string
	if schema == "" {
		stmt = fmt.Sprintf("RENAME %s %s TO %s", t, old, new)
	} else {
		stmt = fmt.Sprintf("RENAME %s %s.%s TO %s.%s", t, schema, old, schema, new)
	}
	_, err = tx.Exec(stmt)
	return err
}

// RenameGlobal changes the global name on the Database
func RenameGlobal(tx *sql.Tx, t, old, new string) error {

	return Rename(tx, t, old, new, "")
}
