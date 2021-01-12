package resource

import (
	"fmt"
	"strings"
)

// DatabaseMeta represents information about Database Objects
type DatabaseMeta struct {
	Schema     string
	ObjectName string
}

// GetMetaFromQNDefault uses a qualified name and returns the schema and name.
// Should name not be qualified fallback to default schema
func GetMetaFromQNDefault(qn, schemaDefault string) (meta DatabaseMeta, err error) {
	parts := strings.SplitN(qn, ".", 2)
	if len(parts) == 0 {
		err = fmt.Errorf("Invalid qualified name: %s", qn)
		return
	}
	if len(parts) == 1 {
		meta.Schema = schemaDefault
		meta.ObjectName = parts[0]
		return
	}

	meta.Schema = parts[0]
	meta.ObjectName = parts[1]
	return
}
