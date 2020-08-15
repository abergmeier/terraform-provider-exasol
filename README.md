![Test & Build](https://github.com/abergmeier/terraform-provider-exasol/workflows/Test%20&%20Build/badge.svg)

# Terraform Plugin for EXASOL

Prototype implementation of Plugin for EXASOL.

## Status

| Supported         | Implemeted as          |
| ---               | ---                    |
| Connection        | exasol_connection      |
| Schema (physical) | exasol_physical_schema |



| Unsupported      | Possible implementation as |
| ---              | ---                        |
| Function         | exasol_function            |
| Role             | exasol_role                |
| Schema (virtual) | exasol_virtual_schema      |
| Script           | exasol_script              |
| Table            | exasol_table               |
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
