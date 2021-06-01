package view

import (
	"context"

	"github.com/abergmeier/terraform-provider-exasol/internal"
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
			"column": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Column which might be used to create View columns",
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
		ReadContext: read,
	}
}

func read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	return readData(d, locked.Conn, ra)
}

func readData(d internal.Data, c *exasol.Conn, args argument.RequiredArguments) diag.Diagnostics {

	vr, err := computed.ReadView(c, args.Schema, args.Name)
	if err != nil {
		return diag.FromErr(err)
	}

	err = vr.SetComment(d)
	if err != nil {
		return diag.FromErr(err)
	}

	err = vr.SetColumns(d)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("subquery", vr.Subquery)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(resource.NewID(args.Schema, args.Name))
	return nil

}
