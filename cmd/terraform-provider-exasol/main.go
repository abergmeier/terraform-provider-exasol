package main

import (
	"github.com/abergmeier/terraform-exasol/internal/resourceprovider"
	"github.com/hashicorp/terraform/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: resourceprovider.Provider,
	})
}