package user_test

import (
	"flag"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	exaConf exasol.ConnConf
)

func TestMain(m *testing.M) {
	flag.Parse()
	exaConf = test.MustCreateConf()
	os.Exit(m.Run())
}
