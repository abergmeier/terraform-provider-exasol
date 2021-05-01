package globallock

import "github.com/hashicorp/terraform-plugin-sdk/v2/diag"

func RunAndRetryRollbacks(fun func() error) diag.Diagnostics {
	for {
		err := fun()
		if IsRollbackError(err) {
			// Ignore error
			continue
		}
		return diag.FromErr(err)
	}
}
