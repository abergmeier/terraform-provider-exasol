package role_test

import (
	"os"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	exaConf exasol.ConnConf
)

func TestMain(m *testing.M) {
	exaConf = internal.MustCreateTestConf()
	os.Exit(m.Run())
}
