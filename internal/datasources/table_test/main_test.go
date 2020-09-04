package table_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-exasol/pkg/db"
)

const (
	schemaName = "datasources_table_TestMain"
)

var (
	exaClient *exaprovider.Client
)

func TestMain(m *testing.M) {

	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaClient = internal.MustCreateTestClient()
	defer exaClient.Close()

	func() {
		locked := exaClient.Lock()
		defer locked.Unlock()
		locked.Conn.Execute(fmt.Sprintf("CREATE SCHEMA %s", schemaName))
		db.MustCommit(locked.Conn)
	}()

	defer func() {
		locked := exaClient.Lock()
		defer locked.Unlock()
		locked.Conn.Execute(fmt.Sprintf("DROP SCHEMA %s", schemaName))
		locked.Conn.Commit()
	}()

	return m.Run()
}
