package test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/resourceprovider"
	"github.com/exasol/exasol-driver-go"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	CheckFailedError = errors.New("Check failed")
)

type DefaultAccProviders struct {
	Exasol    *schema.Provider
	Factories map[string]func() (*schema.Provider, error)
}

func NewDefaultAccProviders() DefaultAccProviders {
	p := resourceprovider.Provider()
	return DefaultAccProviders{
		Exasol: p,
		Factories: map[string]func() (*schema.Provider, error){
			"exasol": func() (*schema.Provider, error) {
				return p, nil
			},
		},
	}
}

type ObjectTest struct {
	ResourceName string
	DbName       string
	Stmt         string
	Config       string
}

func HCLProviderFromConf(dsn *exasol.DSNConfig) string {
	return fmt.Sprintf(`provider "exasol" {
	dsn = "%s"
}`, dsn.String())
}

func False(p *schema.Provider, cb func(*sql.Tx) (bool, error)) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		c := p.Meta().(*exaprovider.Client)
		locked := c.Lock(context.TODO())
		defer locked.Unlock()

		t, err := cb(locked.Tx)
		if err != nil {
			return err
		}

		if t {
			return CheckFailedError
		}

		return nil
	}
}

func True(p *schema.Provider, cb func(*sql.Tx) (bool, error)) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		c := p.Meta().(*exaprovider.Client)
		locked := c.Lock(context.TODO())
		defer locked.Unlock()

		t, err := cb(locked.Tx)
		if err != nil {
			return err
		}

		if !t {
			return CheckFailedError
		}

		return nil
	}
}
