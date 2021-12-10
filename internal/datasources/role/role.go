package role

import (
	"context"
	"database/sql"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Resource returns the Datasource for Exasol Role
func Resource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Role",
			},
		},
		ReadContext: read,
	}
}

func read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	return readData(ctx, d, locked.Tx)
}

func readData(ctx context.Context, d internal.Data, tx *sql.Tx) diag.Diagnostics {
	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err)
	}

	_, err = tx.QueryContext(ctx, "SELECT ROLE_NAME FROM SYS.EXA_ALL_ROLES WHERE ROLE_NAME = ?", name)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(err)
}
