package resourceprovider

import (
	"os"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/exasol/exasol-driver-go"
)

func providerConfigure(d internal.Data) (interface{}, error) {

	var conf *exasol.DSNConfig
	var err error
	dsn := d.Get("dsn").(string)
	if dsn == "" {

		h := d.Get("ip")
		if h == nil {
			h = d.Get("host")
		}
		var host string
		if h == nil {
			host = ""
		} else {
			host = h.(string)
		}
		if host == "" {
			host = os.Getenv("EXAHOST")
		}

		username := d.Get("username").(string)
		if username == "" {
			username = os.Getenv("EXAUID")
		}
		password := d.Get("password").(string)
		if password == "" {
			password = os.Getenv("EXAPWD")
		}
		port := int(d.Get("port").(int))

		conf = &exasol.DSNConfig{
			User:     username,
			Password: password,
			Port:     port,
			Host:     host,
		}
	} else {
		conf, err = exasol.ParseDSN(dsn)
		if err != nil {
			return nil, err
		}
	}

	return exaprovider.NewClient(conf), nil
}
