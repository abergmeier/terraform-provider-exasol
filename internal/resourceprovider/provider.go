package resourceprovider

import (
	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/datasources"
	dtable "github.com/abergmeier/terraform-exasol/internal/datasources/table"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-exasol/internal/resources"
	rtable "github.com/abergmeier/terraform-exasol/internal/resources/table"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

func Provider() terraform.ResourceProvider {
	provider := &schema.Provider{
		DataSourcesMap: map[string]*schema.Resource{
			"exasol_connection":      datasources.ConnectionResource(),
			"exasol_physical_schema": datasources.PhysicalSchema(),
			"exasol_table":           dtable.Resource(),
		},
		ResourcesMap: map[string]*schema.Resource{
			"exasol_connection":      resources.ConnectionResource(),
			"exasol_physical_schema": resources.PhysicalSchema(),
			"exasol_table":           rtable.Resource(),
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
	provider.ConfigureFunc = func(d *schema.ResourceData) (interface{}, error) {
		// Shameless plug from https://github.com/terraform-providers/terraform-provider-aws/blob/d51784148586f605ab30ecea268e80fe83d415a9/aws/provider.go
		terraformVersion := provider.TerraformVersion
		if terraformVersion == "" {
			// Terraform 0.12 introduced this field to the protocol
			// We can therefore assume that if it's missing it's 0.10 or 0.11
			terraformVersion = "0.11+compatible"
		}
		return providerConfigure(d)
	}
	return provider
}

func providerConfigure(d internal.Data) (interface{}, error) {

	conf := exasol.ConnConf{
		Host:     d.Get("ip").(string),
		Port:     uint16(d.Get("port").(int)),
		Username: d.Get("username").(string),
		Password: d.Get("password").(string),
	}

	return exaprovider.NewClient(conf), nil
}
