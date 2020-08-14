package resources

import (
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform/helper/schema"
)

func DataSourceExaConnection() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of connection",
			},
			"to": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Where connection points to",
			},
			"username": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "User with connection",
			},
		},
		Read: readConnection,
	}
}

func ResourceExaConnection() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of connection",
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
	return readConnectionData(d, c)
}

func readConnectionData(d internal.Data, c *exaprovider.Client) error {
	name, err := resourceName(d)
	if err != nil {
		return err
	}

	res, err := c.Conn.FetchSlice("SELECT CONNECTION_NAME, CONNECTION_STRING, USER_NAME, CREATED FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
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
	return createConnectionData(d, c)
}

func createConnectionData(d internal.Data, c *exaprovider.Client) error {
	name, err := resourceName(d)
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
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s'", name, to)
		_, err = c.Conn.Execute(stmt)
	} else if identifiedBy == "" {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err = c.Conn.Execute(stmt)
	} else {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
		_, err = c.Conn.Execute(stmt)
	}

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func deleteConnection(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return deleteConnectionData(d, c)
}

func deleteConnectionData(d internal.Data, c *exaprovider.Client) error {

	name, err := resourceName(d)
	if err != nil {
		return err
	}
	stmt := fmt.Sprintf("DROP CONNECTION %s", name)
	_, err = c.Conn.Execute(stmt)
	return err
}

func importConnection(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	return importConnectionData(d, c)
}

func importConnectionData(d internal.Data, c *exaprovider.Client) ([]*schema.ResourceData, error) {
	name, err := resourceName(d)
	if err != nil {
		return nil, err
	}

	res, err := c.Conn.FetchSlice("SELECT CONNECTION_NAME, CONNECTION_STRING, USER_NAME, CREATED FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")

	if len(res) == 0 {
		return nil, fmt.Errorf("Connection %s not found", name)
	}

	rd := &schema.ResourceData{}

	rd.Set("to", res[0][1].(string))
	username, ok := res[0][2].(string)
	if ok && username != "" {
		rd.Set("username", username)
	}
	password, ok := res[0][3].(string)
	if ok && password != "" {
		rd.Set("password", password)
	}
	rd.SetId(res[0][0].(string))
	return []*schema.ResourceData{rd}, err
}

func updateConnection(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return updateConnectionData(d, c)
}

func updateConnectionData(d internal.Data, c *exaprovider.Client) error {
	name, err := resourceName(d)
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
		_, err := c.Conn.Execute(stmt)
		return err
	}

	if identifiedBy == "" {
		stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err := c.Conn.Execute(stmt)
		return err
	}

	stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
	_, err = c.Conn.Execute(stmt)
	return err
}

func existsConnection(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*exaprovider.Client)
	return existsConnectionData(d, c)
}

func existsConnectionData(d internal.Data, c *exaprovider.Client) (bool, error) {
	name, err := resourceName(d)
	if err != nil {
		return false, err
	}

	result, err := c.Conn.Execute("SELECT CONNECTION_NAME FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", [][]interface{}{
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

func resourceName(d internal.Data) (string, error) {
	name := d.Get("name")
	if name == nil {
		return "", fmt.Errorf("Missing name for %s", d)
	}
	if name == "" {
		return "", fmt.Errorf("Empty name for %s", d)
	}
	return name.(string), nil
}

func resourceTo(d internal.Data) (string, error) {
	to := d.Get("to")
	if to == nil {
		return "", fmt.Errorf("Missing to for %s", d)
	}
	if to == "" {
		return "", fmt.Errorf("Empty name for %s", d)
	}
	return to.(string), nil
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
