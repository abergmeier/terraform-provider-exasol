package view

import (
	"context"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/cached"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
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
				Description: "Name of View",
			},
			"schema": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema that View is in",
			},
			"composite": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Composite which might be used to create View columns",
			},
			"subquery": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Subquery for the View",
			},
			"comment": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Comment of the View",
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

	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err)
	}
	schema, err := argument.Schema(d)
	if err != nil {
		return diag.FromErr(err)
	}

	vr, err := computed.ReadView(c, schema, name)
	if err != nil {
		return diag.FromErr(err)
	}

	err = vr.SetComment(d)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("composite", vr.Composite)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("subquery", vr.Subquery)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(resource.NewID(schema, name))
	return nil

}
