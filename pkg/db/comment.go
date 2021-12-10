package db

import (
	"database/sql"
	"fmt"
)

// Comment changes the comment on the Database object
func Comment(tx *sql.Tx, t, objectName, newComment, schema string) error {

	stmt := fmt.Sprintf("COMMENT ON %s %s.%s IS %s", t, schema, objectName, newComment)
	_, err := tx.Exec(stmt)
	return err
}
