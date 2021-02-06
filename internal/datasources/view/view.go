package view

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
				Description: "Name of View",
			},
			"schema": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema to create View in",
			},
			"composite": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "composite which might be used to create View columns",
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

	vr, err := computed.ReadView(c, schema, name)
	if err != nil {
		return err
	}

	err = vr.SetComment(d)
	if err != nil {
		return err
	}

	err = d.Set("composite", vr.Composite)
	if err != nil {
		return err
	}

	err = d.Set("subquery", vr.Subquery)
	if err != nil {
		return err
	}

	d.SetId(resource.NewID(schema, name))
	return nil

}
