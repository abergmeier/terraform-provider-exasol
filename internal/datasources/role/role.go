package role

import (
	"context"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/cached"
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
		Exists: func(d *schema.ResourceData, meta interface{}) (bool, error) {
			return cached.Exists(exists, d, meta)
		},
		ReadContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return cached.ReadContext(read, ctx, d, meta)
		},
	}
}

func exists(d *schema.ResourceData, conn *exaprovider.Connection) (bool, error) {
	return existsData(d, conn.Conn)
}

func existsData(d binding.Data, c binding.Conn) (bool, error) {
	name, err := argument.Name(d)
	if err != nil {
		return false, err
	}

	return Exists(c, name)
}

// Exists checks whether the Role exists
func Exists(c binding.Conn, name string) (bool, error) {
	res, err := c.FetchSlice("SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE UPPER(ROLE_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(res) != 0, nil
}

func read(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	return readData(d, conn.Conn)
}

func readData(d binding.Data, c *exasol.Conn) diag.Diagnostics {
	name, err := argument.Name(d)
	if err != nil {
		return diag.FromErr(err)
	}
	_, err = c.FetchSlice("SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE ROLE_NAME = ?", []interface{}{
		name,
	}, "SYS")
	return diag.FromErr(err)
}
