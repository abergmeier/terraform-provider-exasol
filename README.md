![Test & Build](https://github.com/abergmeier/terraform-provider-exasol/workflows/Test%20&%20Build/badge.svg)

# Terraform Plugin for EXASOL

Prototype implementation of Plugin for EXASOL.

Go [here](https://registry.terraform.io/providers/abergmeier/exasol/latest) for Terraform Registry.

## Usage in Terraform 0.13

To use the provider, you currently have to add it to your terraform definitions:

```
terraform {
  required_providers {
    exasol = {
      source = "abergmeier/exasol"
    }
  }
}
```

## Usage in Terraform 0.12

In Terraform 0.12 it is easiest to copy the binary directly alongside your terraform definitions:

Thus your directory should look something like this:

```
schema.tf
table.tf
terraform-provider-exasol
```

## Status

| Supported         | Implemeted as          | Examples                                             |
| ---               | ---                    | ---                                                  |
| Connection        | exasol_connection      | [deployment/connection.tf](deployment/connection.tf) |
| Schema (physical) | exasol_physical_schema | [deployment/schema.tf](deployment/schema.tf)         |
| Table             | exasol_table           | [deployment/table.tf](deployment/table.tf)         |



| Unsupported      | Possible implementation as |
| ---              | ---                        |
| Function         | exasol_function            |
| Role             | exasol_role                |
| Schema (virtual) | exasol_virtual_schema      |
| Script           | exasol_script              |
| User             | exasol_user                |
| View             | exasol_view                |


## Testing

To test call

```
EXAHOST=<exasolserver> scripts/test.sh.
```

## Credits

This provider was made possible due to the following shoulders: https://github.com/GrantStreetGroup/go-exasol-client
Cheers
