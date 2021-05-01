package schema

import (
	"context"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/cached"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/grantstreetgroup/go-exasol-client"
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
		ReadContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return cached.ReadContext(read, ctx, d, meta)
		},
	}
}

func read(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	return readPhysicalSchemaData(d, conn.Conn)
}

func readPhysicalSchemaData(d binding.Data, c *exasol.Conn) diag.Diagnostics {
	name := d.Get("name").(string)

	res, err := c.FetchSlice("SELECT SCHEMA_NAME FROM EXA_ALL_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = FALSE ", []interface{}{
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
