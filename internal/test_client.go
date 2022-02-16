package internal

import (
	"os"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/exasol/exasol-driver-go"
)

func MustCreateTestConf() *exasol.DSNConfig {
	exaHost := os.Getenv("EXAHOST")
	if exaHost == "" {
		exaHost = "localhost"
	}

	exaUID := os.Getenv("EXAUID")
	if exaUID == "" {
		exaUID = "sys"
	}

	exaPWD := os.Getenv("EXAPWD")
	if exaPWD == "" {
		exaPWD = "exasol"
	}

	autocommit := false
	validate := false

	return &exasol.DSNConfig{
		User:                      exaUID,
		Password:                  exaPWD,
		Host:                      exaHost,
		Port:                      8563,
		Autocommit:                &autocommit,
		ValidateServerCertificate: &validate,
	}
	//LogLevel: "debug",
}

func MustCreateTestClient() *exaprovider.Client {
	return exaprovider.NewClient(MustCreateTestConf())
}
