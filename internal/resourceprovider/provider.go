package resourceprovider

import (
	"context"
	"fmt"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/datasources"
	dconn "github.com/abergmeier/terraform-provider-exasol/internal/datasources/connection"
	drole "github.com/abergmeier/terraform-provider-exasol/internal/datasources/role"
	dtable "github.com/abergmeier/terraform-provider-exasol/internal/datasources/table"
	dview "github.com/abergmeier/terraform-provider-exasol/internal/datasources/view"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/resources"
	rconn "github.com/abergmeier/terraform-provider-exasol/internal/resources/connection"
	rrole "github.com/abergmeier/terraform-provider-exasol/internal/resources/role"
	rtable "github.com/abergmeier/terraform-provider-exasol/internal/resources/table"
	ruser "github.com/abergmeier/terraform-provider-exasol/internal/resources/user"
	"github.com/abergmeier/terraform-provider-exasol/internal/secretservice"
	"github.com/grantstreetgroup/go-exasol-client"
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
		},
		Schema: map[string]*schema.Schema{
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("EXAUID", nil),
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("EXAPWD", nil),
			},
			"ip": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("EXAHOST", nil),
			},
			"port": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  8563,
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

func providerConfigure(d internal.Data) (interface{}, error) {

	conf := exasol.ConnConf{
		Host:     d.Get("ip").(string),
		Port:     uint16(d.Get("port").(int)),
		Username: d.Get("username").(string),
	}

	pd := d.Get("password")

	if pd != nil {
		conf.Password = pd.(string)
	}

	if conf.Password == "" {
		passwords, err := secretservice.SearchPassword(fmt.Sprintf("%s:%d", conf.Host, conf.Port), conf.Username)
		if err == nil {
			conf.Password = passwords[0].Value
		} else {
			fmt.Printf("Ignoring SecretService lookup error: %s\n", err)
		}
	}

	return exaprovider.NewClient(conf), nil
}
