package resources

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// PhysicalSchema returns the schema.Resource for managing a non-virtual Schema
func PhysicalSchema() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema",
			},
		},
		CreateContext: createPhysicalSchema,
		ReadContext:   readPhysicalSchema,
		UpdateContext: updatePhysicalSchema,
		DeleteContext: deletePhysicalSchema,
		Importer: &schema.ResourceImporter{
			StateContext: importPhysicalSchema,
		},
	}
}

func createPhysicalSchema(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := createPhysicalSchemaData(d, locked.Tx)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Tx.Commit())
}

func createPhysicalSchemaData(d internal.Data, tx *sql.Tx) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE SCHEMA %s", name)
	_, err = tx.Exec(stmt)

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func deletePhysicalSchema(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := deletePhysicalSchemaData(d, locked.Tx)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Tx.Commit())
}

func deletePhysicalSchemaData(d internal.Data, tx *sql.Tx) error {
	name := d.Get("name").(string)

	stmt := fmt.Sprintf("DROP SCHEMA %s", name)
	_, err := tx.Exec(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func importPhysicalSchema(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := importPhysicalSchemaData(ctx, d, locked.Tx)
	if err != nil {
		return nil, err
	}
	err = locked.Tx.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importPhysicalSchemaData(ctx context.Context, d internal.Data, tx *sql.Tx) error {

	r, err := tx.QueryContext(ctx, "SELECT SCHEMA_NAME FROM SYS.EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = false", d.Id())
	if err != nil {
		return err
	}

	if !r.Next() {
		return fmt.Errorf("schema %s not found", d.Id())
	}
	d.SetId(strings.ToUpper(d.Id()))
	return nil
}

func readPhysicalSchema(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	return readPhysicalSchemaTx(ctx, d, locked.Tx)
}

func readPhysicalSchemaTx(ctx context.Context, d internal.Data, tx *sql.Tx) diag.Diagnostics {
	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err)
	}

	res, err := tx.QueryContext(ctx, "SELECT SCHEMA_NAME FROM SYS.EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = FALSE ", name)
	if err != nil {
		return diag.FromErr(err)
	}

	if !res.Next() {
		return diag.Errorf("Schema %s not found", name)
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func updatePhysicalSchema(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := updatePhysicalSchemaData(d, locked.Tx)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Tx.Commit())
}

func updatePhysicalSchemaData(d internal.Data, tx *sql.Tx) error {

	if d.HasChange("name") {
		old, new := d.GetChange("name")
		err := db.Rename(tx, "SCHEMA", old.(string), new.(string), "")
		if err != nil {
			return err
		}

		d.Set("name", new)
	}

	_ = d.Get("name").(string)
	return nil
}
