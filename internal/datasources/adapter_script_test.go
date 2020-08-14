package datasources

import (
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/resources"
)

func TestReadAdapterScript(t *testing.T) {
	schemaName := t.Name()

	schemaCreate := &internal.TestData{
		Values: map[string]interface{}{
			"name": schemaName,
		},
	}

	resources.CreatePhysicalSchemaData(schemaCreate, exaClient)

	defer resources.DeletePhysicalSchemaData(schemaCreate, exaClient)

	stmt := `CREATE JAVA ADAPTER SCRIPT my_script AS
	%jar hive_jdbc_adapter.jar;
`
	_, err := exaClient.Conn.Execute(stmt, [][]interface{}{}, schemaName)
	if err != nil {
		t.Fatal(err)
	}
}
