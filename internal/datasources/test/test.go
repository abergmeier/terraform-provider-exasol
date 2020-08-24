package test

import (
	"github.com/abergmeier/terraform-exasol/internal/resourceprovider"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

var (
	// DefaultAccProviders are all Providers AKA exasol
	DefaultAccProviders map[string]terraform.ResourceProvider
)

func init() {
	testAccProvider := resourceprovider.Provider().(*schema.Provider)
	DefaultAccProviders = map[string]terraform.ResourceProvider{
		"exasol": testAccProvider,
	}
}
