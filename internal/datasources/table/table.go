package table

import (
	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
	"github.com/grantstreetgroup/go-exasol-client"
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
			"column_indices":      computed.ColumnIndicesSchema(),
			"columns":             computed.ColumnsSchema(),
			"foreign_key_indices": computed.ForeignKeysSchema(),
			"primary_key_indices": computed.PrimaryKeysSchema(),
		},
		Read: read,
	}
}

func read(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return readData(d, locked.Conn)
}

func readData(d internal.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	schema, err := argument.Schema(d)
	if err != nil {
		return err
	}

	tr, err := computed.ReadTable(c, schema, name)
	if err != nil {
		return err
	}

	err = d.Set("columns", tr.Columns)
	if err != nil {
		return err
	}

	err = d.Set("column_indices", tr.ColumnIndices)
	if err != nil {
		return err
	}

	err = d.Set("primary_key_indices", tr.PrimaryKeys)
	if err != nil {
		return err
	}

	err = d.Set("foreign_key_indices", tr.ForeignKeys)
	if err != nil {
		return err
	}

	d.SetId(resource.NewID(schema, name))
	return nil

}
