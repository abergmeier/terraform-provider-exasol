package role

import (
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Resource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Role",
			},
		},
		Create: create,
		Update: update,
		Delete: delete,
		Exists: exists,
		Importer: &schema.ResourceImporter{
			State: imp,
		},
		Read: read,
	}
}

func create(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := createData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func createData(d internal.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE ROLE %s", name)
	_, err = c.Execute(stmt)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(name))
	return err
}

func delete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := deleteData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func deleteData(d internal.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP ROLE %s", name)
	_, err = c.Execute(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
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

func Exists(c internal.Conn, name string) (bool, error) {
	res, err := c.FetchSlice("SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE UPPER(ROLE_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(res) != 0, nil
}

func imp(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := importData(d, locked.Conn)
	if err != nil {
		return nil, err
	}
	err = locked.Conn.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(d internal.Data, c *exasol.Conn) error {
	name := d.Id()
	if name == "" {
		return errors.New("Import expects id to be set")
	}
	name = strings.ToUpper(name)
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	return readData(d, c)
}

func read(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return readData(d, locked.Conn)
}

func readData(d internal.Data, c *exasol.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	_, err = c.FetchSlice("SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE ROLE_NAME = ?", []interface{}{
		name,
	}, "SYS")
	return err
}

func update(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := updateData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func updateData(d internal.Data, c *exasol.Conn) error {

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(c, "ROLE", old.(string), new.(string))
		if err != nil {
			return err
		}
	}

	return readData(d, c)
}
