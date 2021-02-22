// +build !windows AND !freebsd

package resourceprovider

import (
	"fmt"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/secretservice"
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
