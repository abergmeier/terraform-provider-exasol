package computed

import "github.com/abergmeier/terraform-provider-exasol/internal/binding"

func setComment(c string, d binding.Data) error {
	if c == "" {
		return d.Set("comment", nil)
	} else {
		return d.Set("comment", c)
	}
}
