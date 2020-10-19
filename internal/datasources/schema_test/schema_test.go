package schema_test

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccExasolSchema_basic(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
`, test.HCLProviderFromConf(locked.Conn.Conf), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testName("data.exasol_physical_schema.dummy"),
				),
			},
		},
	})
}

func testName(id string) resource.TestCheckFunc {
	return func(state *terraform.State) error {
		ds, ok := state.RootModule().Resources[id]
		if !ok {
			return fmt.Errorf("Datasource not found: %s", id)
		}

		name, ok := ds.Primary.Attributes["name"]
		if !ok {
			return fmt.Errorf("Not found: %s.name", id)
		}

		if name != schemaName {
			return fmt.Errorf("Expected Schema name %s: %s", schemaName, name)
		}

		return nil
	}
}
