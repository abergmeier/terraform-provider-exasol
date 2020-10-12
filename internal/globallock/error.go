// Package globallock is handling global lock behavior in Exasol.
// Mutations on Connections and Users are not allowed to
// happen in parallel and Exasol will automatically rollback
// the Transaction.
package globallock

import (
	"strings"
)

// IsRollbackError checks whether there is an error
// and whether the error is due to an Exasol
// rollback
func IsRollbackError(err error) bool {
	if err == nil {
		return false
	}

	// Currently there seems to be no better way than to compare error strings
	return strings.HasPrefix(err.Error(), "GlobalTransactionRollback msg: Transaction collision: automatic transaction rollback.")
}
