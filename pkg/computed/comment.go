package computed

import "github.com/abergmeier/terraform-provider-exasol/internal"

func setComment(c string, d internal.Data) error {
	if c == "" {
		return d.Set("comment", nil)
	} else {
		return d.Set("comment", c)
	}
}
