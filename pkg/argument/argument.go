package argument

import (
	"errors"
	"fmt"

	"github.com/abergmeier/terraform-exasol/internal"
)

var (
	// ErrorEmptyName is an error because name has to have a meaningful value
	ErrorEmptyName = errors.New("Empty name not allowed")
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
