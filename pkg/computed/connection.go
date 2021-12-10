package computed

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
)

// ReadConnection reads all attributes from Database.
// Will return EmptyConnectionName for empty name
func ReadConnection(ctx context.Context, d internal.Data, tx *sql.Tx) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	r, err := tx.QueryContext(ctx, "SELECT CONNECTION_STRING, USER_NAME, CREATED FROM SYS.EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", name)
	if err != nil {
		return err
	}

	if !r.Next() {
		return fmt.Errorf("connection %s not found in Database", name)
	}

	var to string
	var username interface{}
	var created interface{}
	err = r.Scan(&to, &username, &created)
	if err != nil {
		return err
	}
	err = d.Set("to", to)
	if err != nil {
		return err
	}
	if username == nil {
		return d.Set("username", "")
	} else {
		return d.Set("username", username.(string))
	}
}
