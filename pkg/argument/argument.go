package argument

import (
	"fmt"

	"github.com/abergmeier/terraform-exasol/internal"
)

// Name extracts name of Data
func Name(d internal.Data) (string, error) {
	name := d.Get("name").(string)
	if name == "" {
		return "", fmt.Errorf("Empty name for %s", d)
	}
	return name, nil
}

// Schema extracts schema of Data
func Schema(d internal.Data) (string, error) {
	name := d.Get("schema").(string)
	if name == "" {
		return "", fmt.Errorf("Empty schema for %s", d)
	}
	return name, nil
}
