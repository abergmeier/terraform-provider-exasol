package db

import "database/sql"

// MustCommit tries commit and fails hard if it does not work
func MustCommit(tx *sql.Tx) {
	err := tx.Commit()
	if err != nil {
		panic(err)
	}
}
