package connection_test

import (
	"flag"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/exasol/exasol-driver-go"
)

var (
	exaClient *exaprovider.Client
	exaConf   *exasol.DSNConfig
)

func init() {
	exaConf = internal.MustCreateTestConf()
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaClient = exaprovider.NewClient(exaConf)

	return m.Run()
}
