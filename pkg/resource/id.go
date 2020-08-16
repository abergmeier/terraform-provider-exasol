package resource

import (
	"fmt"
	"strings"
)

// NewID creates new absolute id for Terraform
func NewID(schema, name string) string {
	return fmt.Sprintf("%s.%s", strings.ToUpper(schema), strings.ToUpper(name))
}

// SplitIDInSchema takes an id prefixed by Schema and extracts the different
// parts
func SplitIDInSchema(id string) (schema, name string, err error) {
	parts := strings.SplitN(id, ".", 2)
	if len(parts) < 1 {
		return "", "", fmt.Errorf("%s is missing Schema", id)
	}
	schema = parts[0]
	if len(parts) < 2 {
		return "", "", fmt.Errorf("%s is missing Name", id)
	}
	name = parts[1]
	return
}
