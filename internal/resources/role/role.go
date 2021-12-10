package role

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
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
		CreateContext: createRole,
		UpdateContext: updateRole,
		DeleteContext: delete,
		Importer: &schema.ResourceImporter{
			StateContext: imp,
		},
		ReadContext: readRole,
	}
}

func createRole(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := createData(d, locked.Tx)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Tx.Commit())
}

func createData(d internal.Data, tx *sql.Tx) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE ROLE %s", name)
	_, err = tx.Exec(stmt)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(name))
	return err
}

func delete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := deleteData(d, locked.Tx)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Tx.Commit())
}

func deleteData(d internal.Data, tx *sql.Tx) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP ROLE %s", name)
	_, err = tx.Exec(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func imp(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := importData(ctx, d, locked.Tx)
	if err != nil {
		return nil, err
	}
	err = locked.Tx.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(ctx context.Context, d internal.Data, tx *sql.Tx) error {
	name := d.Id()
	if name == "" {
		return errors.New("import expects id to be set")
	}
	name = strings.ToUpper(name)
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	_, err = readData(ctx, d, tx)
	return err
}

func readRole(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	diags, _ := readData(ctx, d, locked.Tx)
	return diags
}

func readData(ctx context.Context, d internal.Data, tx *sql.Tx) (diag.Diagnostics, error) {
	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err), err
	}
	_, err = tx.ExecContext(ctx, "SELECT ROLE_NAME FROM SYS.EXA_ALL_ROLES WHERE ROLE_NAME = ?", name)
	return diag.FromErr(err), err
}

func updateRole(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	diags := updateData(ctx, d, locked.Tx)
	if diags.HasError() {
		return diags
	}
	err := locked.Tx.Commit()
	return append(diags, diag.FromErr(err)...)
}

func updateData(ctx context.Context, d internal.Data, tx *sql.Tx) diag.Diagnostics {

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(tx, "ROLE", old.(string), new.(string))
		if err != nil {
			return diag.FromErr(err)
		}
	}

	diags, _ := readData(ctx, d, tx)
	return diags
}
