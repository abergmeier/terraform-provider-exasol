package computed

import (
	"fmt"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
)

// ReadConnection reads all attributes from Database.
// Will return EmptyConnectionName for empty name
func ReadConnection(d binding.Data, c binding.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
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
	return d.Set("username", username)
}
