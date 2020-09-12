package root

import (
	"errors"

	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	// ErrorNameAttributeNotFound indicates that the Resource name
	// was not found in root state
	ErrorNameAttributeNotFound = errors.New("Attribute name not found")
	// ErrorResourceNotFound indicates that the searched Resource name
	// was not found in root state
	ErrorResourceNotFound = errors.New("Resource not found")
)

// NameAttribute returns name attribute
// If Resource cannot be found by resource name it returns ErrorNameAttributeNotFound
func NameAttribute(s *terraform.State, resourceName string) (string, error) {
	rs, err := ResourceByName(s, resourceName)
	if err != nil {
		return "", err
	}

	actualName, ok := rs.Primary.Attributes["name"]
	if !ok {
		return "", ErrorNameAttributeNotFound
	}

	return actualName, nil
}

// ResourceByName returns the found root Resource
func ResourceByName(s *terraform.State, resourceName string) (*terraform.ResourceState, error) {

	rs, ok := s.RootModule().Resources[resourceName]
	if !ok {
		return nil, ErrorResourceNotFound
	}

	return rs, nil
}
