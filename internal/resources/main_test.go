package resources

import (
	"os"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
)

var (
	exaClient *exaprovider.Client
)

func TestMain(m *testing.M) {

	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaClient = internal.MustCreateTestClient()
	defer exaClient.Close()

	return m.Run()
}
