package internal

import (
	"os"

	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/grantstreetgroup/go-exasol-client"
)

func MustCreateTestConf() exasol.ConnConf {
	exaHost := os.Getenv("EXAHOST")
	if exaHost == "" {
		panic("Tests need EXAHOST to run")
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

	return exasol.ConnConf{
		Host:     exaHost,
		Port:     8563,
		Username: exaUID,
		Password: exaPWD,
		//LogLevel: "debug",
	}
}

func MustCreateTestClient() *exaprovider.Client {
	return exaprovider.NewClient(MustCreateTestConf())
}
