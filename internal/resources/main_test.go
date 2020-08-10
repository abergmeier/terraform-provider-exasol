package resources

import (
	"os"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	exaClient *exaprovider.Client
)

func TestMain(m *testing.M) {
	exaHost := os.Getenv("EXAHOST")
	if exaHost == "" {
		println("Tests need EXAHOST to run")
		os.Exit(1)
	}

	exaUID := os.Getenv("EXAUID")
	if exaUID == "" {
		println("Set EXAUID to sys")
		exaUID = "sys"
	}

	exaPWD := os.Getenv("EXAPWD")
	if exaPWD == "" {
		println("Set EXAPWD to exasol")
		exaPWD = "exasol"
	}

	conf := exasol.ConnConf{
		Host:     exaHost,
		Port:     8563,
		Username: exaUID,
		Password: exaPWD,
		LogLevel: "debug",
	}

	conn := exasol.Connect(conf)
	exaClient = &exaprovider.Client{
		Conn: conn,
	}

	os.Exit(m.Run())
}
