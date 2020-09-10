package role_test

import (
	"flag"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	exaConf exasol.ConnConf
)

func TestMain(m *testing.M) {
	flag.Parse()
	exaConf = internal.MustCreateTestConf()
	os.Exit(m.Run())
}
