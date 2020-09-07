package computed

import (
	"errors"
	"fmt"

	"github.com/abergmeier/terraform-exasol/internal"
)

var (
	// EmptyConnectionNameError is an error because name has to have a meaningful value
	EmptyConnectionNameError = errors.New("Empty Connection name not allowed")
)

// ReadConnection reads all attributes from Database.
// Might return EmptyConnectionName for empty name
func ReadConnection(d internal.Data, c internal.Conn) error {

	name := d.Get("name").(string)

	if name == "" {
		return EmptyConnectionNameError
	}

	res, err := c.FetchSlice("SELECT CONNECTION_STRING, USER_NAME, CREATED FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("Connection %s not found in Database", name)
	}

	err = d.Set("to", res[0][0].(string))
	if err != nil {
		return err
	}
	username, _ := res[0][1].(string)
	if username != "" {
		err = d.Set("username", username)
	}
	return err
}
