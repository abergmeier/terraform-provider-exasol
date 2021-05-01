package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/cached"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/grantstreetgroup/go-exasol-client"
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
		Create: func(d *schema.ResourceData, meta interface{}) error {
			return cached.Create(create, d, meta)
		},
		ReadContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return cached.ReadContext(read, ctx, d, meta)
		},
		Update: func(d *schema.ResourceData, meta interface{}) error {
			return cached.Update(update, d, meta)
		},
		Delete: func(d *schema.ResourceData, meta interface{}) error {
			return cached.Delete(delete, d, meta)
		},
		Exists: func(d *schema.ResourceData, meta interface{}) (bool, error) {
			return cached.Exists(exists, d, meta)
		},
		Importer: &schema.ResourceImporter{
			State: func(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				return cached.ImporterState(imp, d, meta)
			},
		},
	}
}

func create(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := createPhysicalSchemaData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func createPhysicalSchemaData(d binding.Data, c *exasol.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE SCHEMA %s", name)
	_, err = c.Execute(stmt)

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func delete(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := deletePhysicalSchemaData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func deletePhysicalSchemaData(d binding.Data, c *exasol.Conn) error {
	name := d.Get("name").(string)

	stmt := fmt.Sprintf("DROP SCHEMA %s", name)
	_, err := c.Execute(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func exists(d *schema.ResourceData, conn *exaprovider.Connection) (bool, error) {
	return existsPhysicalSchemaData(d, conn.Conn)
}

func existsPhysicalSchemaData(d binding.Data, c *exasol.Conn) (bool, error) {

	result, err := c.FetchSlice("SELECT SCHEMA_NAME FROM EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?)", []interface{}{
		d.Id(),
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(result) == 1, nil
}

func imp(d *schema.ResourceData, conn *exaprovider.Connection) ([]*schema.ResourceData, error) {
	err := importPhysicalSchemaData(d, conn.Conn)
	if err != nil {
		return nil, err
	}
	err = conn.Conn.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importPhysicalSchemaData(d binding.Data, c *exasol.Conn) error {

	slice, err := c.FetchSlice("SELECT SCHEMA_NAME FROM EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = false", []interface{}{
		d.Id(),
	}, "SYS")
	if err != nil {
		return err
	}

	if len(slice) == 0 {
		return fmt.Errorf("schema %s not found", d.Id())
	}
	d.SetId(strings.ToUpper(d.Id()))
	return nil
}

func read(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	return readPhysicalSchemaData(d, conn.Conn)
}

func readPhysicalSchemaData(d binding.Data, c *exasol.Conn) diag.Diagnostics {
	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err)
	}

	res, err := c.FetchSlice("SELECT SCHEMA_NAME FROM EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = FALSE ", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return diag.FromErr(err)
	}

	if len(res) == 0 {
		return diag.Errorf("Schema %s not found", name)
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func update(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := updatePhysicalSchemaData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func updatePhysicalSchemaData(d binding.Data, c *exasol.Conn) error {

	if d.HasChange("name") {
		old, new := d.GetChange("name")
		err := db.Rename(c, "SCHEMA", old.(string), new.(string), "")
		if err != nil {
			return err
		}

		d.Set("name", new)
	}

	_ = d.Get("name").(string)
	return nil
}
