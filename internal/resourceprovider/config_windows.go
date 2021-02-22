// +build windows

package resourceprovider

import (
	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/grantstreetgroup/go-exasol-client"
)

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

	return exaprovider.NewClient(conf), nil
}
