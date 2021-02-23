package computed

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	reg = regexp.MustCompile(`CREATE\s+(?:OR\s+REPLACE|FORCE)?\s?VIEW\s+.*?AS\s+(.*)(?:\s+COMMENT\s+IS.*)?`)
)

type View struct {
	Comment   string
	Composite string
	Subquery  string
}

func (v *View) SetComment(d internal.Data) error {
	return setComment(v.Comment, d)
}

func ReadView(c *exasol.Conn, schema, name string) (*View, error) {
	stmt := "SELECT VIEW_COMMENT, VIEW_TEXT FROM EXA_ALL_VIEWS WHERE UPPER(VIEW_SCHEMA) = UPPER(?) AND UPPER(VIEW_NAME) = UPPER(?)"
	res, err := c.FetchSlice(stmt, []interface{}{
		schema,
		name,
	}, "SYS")
	if err != nil {
		return nil, fmt.Errorf("Selecting View Metadata for %s.%s failed: %s", schema, name, err)
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("Selecting View Metadata for %s.%s resulted in no result", schema, name)
	}

	row := res[0]
	comment, err := readViewComment(row)
	if err != nil {
		return nil, err
	}

	composite, err := readViewComposite(c, schema, name)
	if err != nil {
		return nil, err
	}

	subquery, err := readViewSubquery(row)
	if err != nil {
		return nil, err
	}

	return &View{
		Comment:   comment,
		Composite: composite,
		Subquery:  subquery,
	}, nil
}

func readViewComment(row []interface{}) (string, error) {

	if row[0] == nil {
		return "", nil // No comment
	}

	return row[0].(string), nil
}

func readViewComposite(c *exasol.Conn, schema, view string) (string, error) {
	stmt := `SELECT COLUMN_NAME, COLUMN_COMMENT
FROM EXA_ALL_COLUMNS
WHERE UPPER(COLUMN_SCHEMA) = UPPER(?) AND UPPER(COLUMN_TABLE) = UPPER(?)
ORDER BY COLUMN_ORDINAL_POSITION`
	res, err := c.FetchSlice(stmt, []interface{}{
		schema,
		view,
	})
	if err != nil {
		return "", fmt.Errorf("Selecting VIEW Columns for %s.%s failed: %s", schema, view, err)
	}

	b := &strings.Builder{}
	for _, row := range res {
		b.WriteString(row[0].(string))
		if row[1] == nil {
			b.WriteString(",\n")
			continue
		}
		comment := row[1].(string)
		if comment == "" {
			b.WriteString(",\n")
		} else {
			fmt.Fprintf(b, " COMMENT IS '%s',\n", comment)
		}
	}

	return b.String(), nil
}

func readViewSubquery(row []interface{}) (string, error) {
	text := row[1].(string)

	submatch := reg.FindSubmatch([]byte(text))
	if len(submatch) == 0 {
		return "", fmt.Errorf("Regex matching Views CREATE text failed: %s", text)
	}
	if len(submatch) != 2 {
		return "", fmt.Errorf("Extracting subquery failed: %s", text)
	}
	return string(submatch[1]), nil
}
