package role

import (
	"context"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/grantstreetgroup/go-exasol-client"
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
	locked := c.Lock()
	defer locked.Unlock()
	return readData(d, locked.Conn)
}

func readData(d internal.Data, c *exasol.Conn) diag.Diagnostics {
	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err)
	}
	_, err = c.FetchSlice("SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE ROLE_NAME = ?", []interface{}{
		name,
	}, "SYS")
	return diag.FromErr(err)
}
