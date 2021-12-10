package resourceprovider

import (
	"fmt"
	"os"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/exasol/exasol-driver-go"
)

func providerConfigure(d internal.Data) (interface{}, error) {

	var conf *exasol.DSNConfig

	dsn := d.Get("dsn")
	if dsn == "exa:localhost:8563;user=sys;password=exasol;autocommit=0;validateservercertificate=0" {
		conf = exasol.NewConfig("sys", "exasol").Autocommit(false).ValidateServerCertificate(false)
	} else if dsn == "" {

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

		conf = exasol.NewConfig(username, password).Port(port).Host(host)
	} else {
		panic(fmt.Sprintf(`Unsupported: %s
supported: exa:localhost:8563;user=sys;password=exasol;autocommit=0;validateservercertificate=0
`, dsn))
	}

	return exaprovider.NewClient(conf), nil
}
