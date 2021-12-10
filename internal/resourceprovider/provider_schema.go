package resourceprovider

import (
	"context"

	"github.com/abergmeier/terraform-provider-exasol/internal/datasources"
	dconn "github.com/abergmeier/terraform-provider-exasol/internal/datasources/connection"
	drole "github.com/abergmeier/terraform-provider-exasol/internal/datasources/role"
	dtable "github.com/abergmeier/terraform-provider-exasol/internal/datasources/table"
	dview "github.com/abergmeier/terraform-provider-exasol/internal/datasources/view"
	"github.com/abergmeier/terraform-provider-exasol/internal/resources"
	rconn "github.com/abergmeier/terraform-provider-exasol/internal/resources/connection"
	rrole "github.com/abergmeier/terraform-provider-exasol/internal/resources/role"
	rtable "github.com/abergmeier/terraform-provider-exasol/internal/resources/table"
	ruser "github.com/abergmeier/terraform-provider-exasol/internal/resources/user"
	rview "github.com/abergmeier/terraform-provider-exasol/internal/resources/view"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Provider() *schema.Provider {
	provider := &schema.Provider{
		DataSourcesMap: map[string]*schema.Resource{
			"exasol_connection":      dconn.Resource(),
			"exasol_physical_schema": datasources.PhysicalSchema(),
			"exasol_role":            drole.Resource(),
			"exasol_table":           dtable.Resource(),
			"exasol_view":            dview.Resource(),
		},
		ResourcesMap: map[string]*schema.Resource{
			"exasol_connection":      rconn.Resource(),
			"exasol_physical_schema": resources.PhysicalSchema(),
			"exasol_role":            rrole.Resource(),
			"exasol_table":           rtable.Resource(),
			"exasol_user":            ruser.Resource(),
			"exasol_view":            rview.Resource(),
		},
		Schema: map[string]*schema.Schema{
			"username": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"password": {
				Type:      schema.TypeString,
				Optional:  true,
				Sensitive: true,
			},
			"host": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"ip": {
				Type:       schema.TypeString,
				Optional:   true,
				Deprecated: "Attribute ip is deprecated. Use host instead.",
			},
			"port": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  8563,
			},
			"dsn": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"username", "password", "host", "ip"},
				ExactlyOneOf:  []string{"host", "ip", "dsn"},
			},
		},
	}
	provider.ConfigureContextFunc = func(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
		// Shameless plug from https://github.com/terraform-providers/terraform-provider-aws/blob/d51784148586f605ab30ecea268e80fe83d415a9/aws/provider.go
		terraformVersion := provider.TerraformVersion
		if terraformVersion == "" {
			// Terraform 0.12 introduced this field to the protocol
			// We can therefore assume that if it's missing it's 0.10 or 0.11
			terraformVersion = "0.11+compatible"
		}
		m, err := providerConfigure(d)
		return m, diag.FromErr(err)
	}
	return provider
}
