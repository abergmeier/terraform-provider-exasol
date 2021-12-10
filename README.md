
[![Test & Build](https://github.com/abergmeier/terraform-provider-exasol/workflows/Test%20&%20Build/badge.svg)](https://github.com/abergmeier/terraform-provider-exasol/actions?query=workflow%3A%22Test+%26+Build%22+branch%3Amaster)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/abergmeier/terraform-provider-exasol)](https://github.com/abergmeier/terraform-provider-exasol/releases/latest)
[![License](https://img.shields.io/github/license/abergmeier/terraform-provider-exasol)](https://github.com/abergmeier/terraform-provider-exasol/blob/master/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/abergmeier/terraform-provider-exasol/cmd/terraform-provider-exasol.svg)](https://pkg.go.dev/github.com/abergmeier/terraform-provider-exasol/cmd/terraform-provider-exasol)

# Terraform Provider for EXASOL

Provider for EXASOL database objects.
Enables #DBAOps with Exasol.

Go [here](https://registry.terraform.io/providers/abergmeier/exasol/latest) for Terraform Registry.

## Usage in Terraform 0.13+

To use the provider, you currently have to add it to your terraform definitions:

```terraform
# Snippet of provider.tf
terraform {
  required_providers {
    exasol = {
      source = "abergmeier/exasol"
    }
  }
}
```

## Status

| Supported         | Implemented as          | Examples                                               |
| ---               | ---                     | ---                                                    |
| Connection        | exasol_connection       | [deployments/connection.tf](deployments/connection.tf) |
| Role              | exasol_role             | [deployments/role.tf](deployments/role.tf)             |
| Schema (physical) | exasol_physical_schema  | [deployments/schema.tf](deployments/schema.tf)         |
| Table             | exasol_table            | [deployments/table.tf](deployments/table.tf)           |
| User              | exasol_user             | [deployments/user.tf](deployments/user.tf)             |
| View              | exasol_view             | [deployments/view.tf](deployments/view.tf)             |



| Unsupported      | Possible implementation as |
| ---              | ---                        |
| Function         | exasol_function            |
| Schema (virtual) | exasol_virtual_schema      |
| Script           | exasol_script              |


## Testing

To test call

```
EXAHOST=<exasolserver> scripts/test.sh.
```
