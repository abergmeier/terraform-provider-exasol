package connection_test

import (
	"flag"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	exaClient *exaprovider.Client
	exaConf   exasol.ConnConf
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaConf = internal.MustCreateTestConf()
	exaClient = exaprovider.NewClient(exaConf)
	defer exaClient.Close()

	return m.Run()
}
