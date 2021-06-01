package argument

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type RequiredArguments struct {
	Schema string
	Name   string
}

func ExtractRequiredArguments(d *schema.ResourceData) (RequiredArguments, diag.Diagnostics) {
	var diags diag.Diagnostics
	name, err := Name(d)
	if err != nil {
		diags = append(diags, diag.FromErr(err)...)
	}
	schema, err := Schema(d)
	if err != nil {
		diags = append(diags, diag.FromErr(err)...)
	}
	if diags.HasError() {
		return RequiredArguments{}, diags
	}
	return RequiredArguments{
		Schema: schema,
		Name:   name,
	}, diags
}
