package internal

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func RootName(s *terraform.State, id string) (string, error) {
	rs, err := rootResource(s, id)
	if err != nil {
		return "", err
	}

	actualName, ok := rs.Primary.Attributes["name"]
	if !ok {
		return "", fmt.Errorf("Attribute name not found")
	}

	return actualName, nil
}

func rootResource(s *terraform.State, id string) (*terraform.ResourceState, error) {

	rs, ok := s.RootModule().Resources[id]
	if !ok {
		return nil, fmt.Errorf("Role not found: %s", id)
	}

	return rs, nil
}
