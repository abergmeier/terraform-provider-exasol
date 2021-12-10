package user_test

import (
	"flag"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/exasol/exasol-driver-go"
)

var (
	exaConf *exasol.DSNConfig
)

func TestMain(m *testing.M) {
	flag.Parse()
	exaConf = internal.MustCreateTestConf()
	os.Exit(m.Run())
}
