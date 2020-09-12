package connection

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Resource for Exasol Connection
func Resource() *schema.Resource {
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
		CreateContext: createConnection,
		ReadContext:   readConnection,
		UpdateContext: updateConnection,
		DeleteContext: deleteConnection,
		Exists:        exists,
		Importer: &schema.ResourceImporter{
			StateContext: importConnection,
		},
	}

}

func readConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := readConnectionData(d, locked.Conn)
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func readConnectionData(d internal.Data, c internal.Conn) error {

	err := computed.ReadConnection(d, c)
	if errors.Is(err, argument.ErrorEmptyName) {
		return fmt.Errorf("Empty name not allowed for Connection (id: %s)", d.Id())
	}
	return err
}

func createConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := createConnectionData(d, locked.Conn)
	if err != nil {
		return diag.FromErr(err)
	}

	err = locked.Conn.Commit()
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
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

func deleteConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := deleteConnectionData(d, locked.Conn)
	if err != nil {
		return diag.FromErr(err)
	}
	err = locked.Conn.Commit()
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
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

func importConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := importConnectionData(d, locked.Conn)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importConnectionData(d internal.Data, c internal.Conn) error {
	name := d.Id()
	if name == "" {
		return errors.New("Import expects id to be set")
	}
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	return readConnectionData(d, c)
}

func updateConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := updateConnectionData(d, locked.Conn)
	if err != nil {
		return diag.FromErr(err)
	}
	err = locked.Conn.Commit()
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func updateConnectionData(d internal.Data, c *exasol.Conn) error {
	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(c, "CONNECTION", old.(string), new.(string))
		if err != nil {
			return err
		}
	}

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

func Exists(c internal.Conn, name string) (bool, error) {
	rows, err := c.FetchSlice("SELECT CONNECTION_NAME FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(rows) > 0, nil
}

func exists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return existsData(d, locked.Conn)
}

func existsData(d internal.Data, c internal.Conn) (bool, error) {
	name, err := argument.Name(d)
	if err != nil {
		return false, err
	}

	return Exists(c, name)
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
	pwd := d.Get("password")
	if pwd == nil {
		return ""
	}
	return pwd.(string)
}
