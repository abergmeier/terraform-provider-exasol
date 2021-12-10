package datasources

import (
	"context"
	"database/sql"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func PhysicalSchema() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema",
			},
		},
		ReadContext: readPhysicalSchema,
	}
}

func readPhysicalSchema(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	return readPhysicalSchemaData(ctx, d, locked.Tx)
}

func readPhysicalSchemaData(ctx context.Context, d internal.Data, tx *sql.Tx) diag.Diagnostics {
	name := d.Get("name").(string)

	res, err := tx.QueryContext(ctx, "SELECT SCHEMA_NAME FROM SYS.EXA_ALL_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = FALSE ", name)
	if err != nil {
		return diag.FromErr(err)
	}

	if !res.Next() {
		return diag.Errorf("Schema %s not found", name)
	}

	d.SetId(strings.ToUpper(name))
	return nil
}
