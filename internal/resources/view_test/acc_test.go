package view_test

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/statements"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDeleteInBetween(t *testing.T) {

	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	resource.Test(t, resource.TestCase{

		PreCheck: func() {
			dv := statements.DropView{
				Schema: schemaName,
				Name:   name,
			}
			locked := exaClient.Lock()
			defer locked.Unlock()
			dv.Execute(locked.Conn)
		},
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccViewResource(name),
			},
			{
				PreConfig: func() {
					dv := statements.DropView{
						Schema: schemaName,
						Name:   name,
					}
					locked := exaClient.Lock()
					defer locked.Unlock()
					err := dv.Execute(locked.Conn)
					if err != nil {
						t.Fatal(err)
					}
				},
				Config: testAccViewChangedResource(name),
			},
		},
	})
}

func testAccViewResource(name string) string {
	return fmt.Sprintf(`resource "exasol_view" "%s" {
	name     = "%s"
	schema   = "%s"
	subquery = "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS"
}
`, name, name, schemaName)
}

func testAccViewChangedResource(name string) string {
	return fmt.Sprintf(`resource "exasol_view" "%s" {
	name     = "%s"
	schema   = "%s"
	subquery = "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS"
}
`, name, name, schemaName)
}
