![Test & Build](https://github.com/abergmeier/terraform-provider-exasol/workflows/Test%20&%20Build/badge.svg)

# Terraform Plugin for EXASOL

Prototype implementation of Plugin for EXASOL.

## Status

| Supported  | Implemeted as |
| ---        | ---           |
| Connection | exasol_connection |



| Unsupported | Possible implementation as |
| ---         | ---             |
| Function    | exasol_function |
| Role        | exasol_role     |
| Schema      | exasol_schema   |
| Script      | exasol_script   |
| Table       | exasol_table    |
| User        | exasol_user     |
| View        | exasol_view     |


## Testing

To test call

```
EXAHOST=<exasolserver> scripts/test.sh.
```
