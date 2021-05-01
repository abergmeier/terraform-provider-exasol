package role

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/cached"
	"github.com/abergmeier/terraform-provider-exasol/internal/datasources/role"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
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
		Create: func(d *schema.ResourceData, meta interface{}) error {
			return cached.Create(create, d, meta)
		},
		UpdateContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return cached.UpdateContext(update, ctx, d, meta)
		},
		Delete: func(d *schema.ResourceData, meta interface{}) error {
			return cached.Delete(delete, d, meta)
		},
		Exists: func(d *schema.ResourceData, meta interface{}) (bool, error) {
			return cached.Exists(exists, d, meta)
		},
		Importer: &schema.ResourceImporter{
			StateContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				return cached.ImporterStateContext(imp, ctx, d, meta)
			},
		},
		ReadContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return cached.ReadContext(read, ctx, d, meta)
		},
	}
}

func create(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := createData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func createData(d binding.Data, c *exasol.Conn) error {

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

func delete(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := deleteData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func deleteData(d binding.Data, c *exasol.Conn) error {

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

func exists(d *schema.ResourceData, conn *exaprovider.Connection) (bool, error) {
	return existsData(d, conn.Conn)
}

func existsData(d binding.Data, c binding.Conn) (bool, error) {
	name, err := argument.Name(d)
	if err != nil {
		return false, err
	}

	return role.Exists(c, name)
}

func imp(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) ([]*schema.ResourceData, error) {
	err := importData(d, conn.Conn)
	if err != nil {
		return nil, err
	}
	err = conn.Conn.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(d binding.Data, c *exasol.Conn) error {
	name := d.Id()
	if name == "" {
		return errors.New("Import expects id to be set")
	}
	name = strings.ToUpper(name)
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	_, err = readData(d, c)
	return err
}

func read(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	diags, _ := readData(d, conn.Conn)
	return diags
}

func readData(d binding.Data, c *exasol.Conn) (diag.Diagnostics, error) {
	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err), err
	}
	_, err = c.FetchSlice("SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE ROLE_NAME = ?", []interface{}{
		name,
	}, "SYS")
	return diag.FromErr(err), err
}

func update(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	diags := updateData(d, conn.Conn)
	if diags.HasError() {
		return diags
	}
	err := conn.Conn.Commit()
	return append(diags, diag.FromErr(err)...)
}

func updateData(d binding.Data, c *exasol.Conn) diag.Diagnostics {

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(c, "ROLE", old.(string), new.(string))
		if err != nil {
			return diag.FromErr(err)
		}
	}

	diags, _ := readData(d, c)
	return diags
}
