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
	exaClient = internal.MustCreateTestClient()

	os.Exit(m.Run())
}
