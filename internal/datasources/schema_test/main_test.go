package schema_test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
)

const (
	schemaName = "datasources_schema_TestMain"
)

var (
	exaClient *exaprovider.Client
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaClient = test.MustCreateClient()

	func() {
		conn := test.OpenManualConnection(exaClient)
		defer conn.Close()
		conn.Conn.Execute(fmt.Sprintf("CREATE SCHEMA %s", schemaName))
		db.MustCommit(conn.Conn)
	}()

	defer func() {
		conn := test.OpenManualConnection(exaClient)
		defer conn.Close()
		conn.Conn.Execute(fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		conn.Conn.Commit()
	}()

	return m.Run()
}
