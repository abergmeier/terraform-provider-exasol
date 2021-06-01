package table

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
				Description: "Name of Table",
			},
			"schema": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema to create Table in",
			},
			"composite": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "composite which might be used to create table columns",
			},
			"comment": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Comment of the Table",
			},
			"column_indices":      computed.ColumnIndicesSchema(),
			"columns":             computed.ColumnsSchema(),
			"foreign_key_indices": computed.ForeignKeysSchema(),
			"primary_key_indices": computed.PrimaryKeysSchema(),
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

	tr, err := computed.ReadTable(c, args.Schema, args.Name)
	if err != nil {
		return diag.FromErr(err)
	}

	err = tr.SetComment(d)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("columns", tr.Columns)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("column_indices", tr.ColumnIndices)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("composite", tr.Composite)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("primary_key_indices", tr.PrimaryKeys)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("foreign_key_indices", tr.ForeignKeys)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(resource.NewID(args.Schema, args.Name))
	return nil

}
