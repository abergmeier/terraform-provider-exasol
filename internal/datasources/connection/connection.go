package connection

import (
	"context"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/cached"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
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
		ReadContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return cached.ReadContext(read, ctx, d, meta)
		},
	}
}

func read(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	return readData(d, conn.Conn)
}

func readData(d binding.Data, c *exasol.Conn) diag.Diagnostics {

	err := computed.ReadConnection(d, c)
	if err != nil {
		return diag.FromErr(err)
	}
	name := d.Get("name").(string)
	d.SetId(strings.ToUpper(name))
	return nil
}
