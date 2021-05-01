package resourceprovider_test

import (
	"flag"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/test"
)

var (
	exaClient *exaprovider.Client
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaClient = test.MustCreateClient()

	return m.Run()
}
