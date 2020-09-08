package resources

import (
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-exasol/pkg/argument"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ConnectionResource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of connection",
				ForceNew:    true,
			},
			"to": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Where connection points to",
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
				Description: "User to use for connection",
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
				Description: "Password to use for connection",
				Sensitive:   true,
			},
		},
		Create: createConnection,
		Read:   readConnection,
		Update: updateConnection,
		Delete: deleteConnection,
		Exists: existsConnection,
		Importer: &schema.ResourceImporter{
			State: importConnection,
		},
	}

}

func readConnection(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return readConnectionData(d, locked.Conn)
}

func readConnectionData(d internal.Data, c *exasol.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	res, err := c.FetchSlice("SELECT CONNECTION_NAME, CONNECTION_STRING, USER_NAME, CREATED FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")

	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("Connection %s not found", name)
	}

	d.Set("to", res[0][1].(string))
	username, ok := res[0][2].(string)
	if ok && username != "" {
		d.Set("username", username)
	}
	password, ok := res[0][3].(string)
	if ok && password != "" {
		d.Set("password", password)
	}
	d.SetId(res[0][0].(string))
	return nil
}

func createConnection(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := createConnectionData(d, locked.Conn)
	if err != nil {
		return err
	}

	return locked.Conn.Commit()
}

func createConnectionData(d internal.Data, c *exasol.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	to := d.Get("to").(string)

	user := resourceUser(d)
	identifiedBy := resourceIdentifiedBy(d)

	if user == "" {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s'", name, to)
		_, err = c.Execute(stmt)
	} else if identifiedBy == "" {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err = c.Execute(stmt)
	} else {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
		_, err = c.Execute(stmt)
	}

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func deleteConnection(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := deleteConnectionData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func deleteConnectionData(d internal.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP CONNECTION %s", name)
	_, err = c.Execute(stmt)
	if err != nil {
		return err
	}
	d.SetId("")
	return nil
}

func importConnection(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := importConnectionData(d, locked.Conn)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importConnectionData(d internal.Data, c *exasol.Conn) error {
	name := d.Id()
	if name == "" {
		return errors.New("Import expects id to be set")
	}
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	res, err := c.FetchSlice("SELECT CONNECTION_NAME, CONNECTION_STRING, USER_NAME, CREATED FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")

	if len(res) == 0 {
		return fmt.Errorf("Connection %s not found", name)
	}

	err = d.Set("to", res[0][1].(string))
	if err != nil {
		return err
	}
	username, ok := res[0][2].(string)
	if ok && username != "" {
		err = d.Set("username", username)
	}
	return err
}

func updateConnection(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := updateConnectionData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func updateConnectionData(d internal.Data, c *exasol.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	to, err := resourceTo(d)
	if err != nil {
		return err
	}
	user := resourceUser(d)
	identifiedBy := resourceIdentifiedBy(d)

	if user == "" {
		stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s'", name, to)
		_, err := c.Execute(stmt)
		return err
	}

	if identifiedBy == "" {
		stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err := c.Execute(stmt)
		return err
	}

	stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
	_, err = c.Execute(stmt)
	return err
}

func existsConnection(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return existsConnectionData(d, locked.Conn)
}

func existsConnectionData(d internal.Data, c *exasol.Conn) (bool, error) {
	name, err := argument.Name(d)
	if err != nil {
		return false, err
	}

	result, err := c.Execute("SELECT CONNECTION_NAME FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", [][]interface{}{
		{
			name,
		},
	}, "SYS")
	if err != nil {
		return false, err
	}

	results := result["results"].([]interface{})[0].(map[string]interface{})["resultSet"].(map[string]interface{})
	ri := results["numRows"]
	if ri == nil {
		return false, errors.New("numRows is nil")
	}

	rows, ok := ri.(float64)
	if !ok {
		return false, errors.New("numRows not float64")
	}

	return rows > 0.0, nil
}

func resourceTo(d internal.Data) (string, error) {
	to := d.Get("to").(string)
	if to == "" {
		return "", fmt.Errorf("Empty attribute `to` for `%s`", d)
	}
	return to, nil
}

func resourceUser(d internal.Data) string {
	user := d.Get("username")
	if user == nil {
		return ""
	}
	return user.(string)
}

func resourceIdentifiedBy(d internal.Data) string {
	id := d.Get("password")
	if id == nil {
		return ""
	}
	return id.(string)
}
