package resourceprovider

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

	td := &internal.TestData{
		Values: map[string]interface{}{
			"ip":       exaHost,
			"username": exaUID,
			"password": exaPWD,
		},
	}
	c, err := providerConfigure(td)
	if err != nil {
		println(err)
		os.Exit(1)
	}

	exaClient = c.(*exaprovider.Client)

	os.Exit(m.Run())
}
