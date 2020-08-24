package schema_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-exasol/pkg/db"
)

const (
	schemaName = "datasources_schema_TestMain"
)

var (
	exaClient *exaprovider.Client
)

func TestMain(m *testing.M) {
	exaClient = internal.MustCreateTestClient()

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

	os.Exit(m.Run())
}
