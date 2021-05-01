package role_test

import (
	"flag"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	exaClient *exaprovider.Client
	exaConf   exasol.ConnConf
)

func init() {
	exaConf = test.MustCreateConf()
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	c, err := exaprovider.NewClient(exaConf, "")
	if err != nil {
		panic(err)
	}
	exaClient = c

	return m.Run()
}
