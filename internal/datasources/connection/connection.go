package connection

import (
	"context"
	"database/sql"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

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
				Computed:    true,
				Description: "Where connection points to",
			},
			"username": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "User used with connection",
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

	err := computed.ReadConnection(ctx, d, tx)
	if err != nil {
		return diag.FromErr(err)
	}
	name := d.Get("name").(string)
	d.SetId(strings.ToUpper(name))
	return nil
}
