package computed

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/grantstreetgroup/go-exasol-client"
)

var (
	columnReg             = regexp.MustCompile(`(?s)CREATE\s+(?:OR\s+REPLACE|FORCE)?\s?VIEW\s+.*?\s+\((.*)\)\s+AS`)
	subqueryReg           = regexp.MustCompile(`(?s)CREATE\s+(?:OR\s+REPLACE|FORCE)?\s?VIEW\s+.*?AS\s+(.*)(?:\s+COMMENT\s+IS.*)?`)
	ReadViewNoResultError = &readViewNoResultError{}
)

type readViewNoResultError struct {
}

type View struct {
	Comment  string
	Columns  []ViewColumn
	Subquery string
}

type ViewColumn struct {
	Name    string
	Comment string
}

func (err *readViewNoResultError) Error() string {
	return ""
}

func (v *View) SetComment(d internal.Data) error {
	return setComment(v.Comment, d)
}

func (v *View) SetColumns(d internal.Data) error {
	var columns []interface{}
	for _, v := range v.Columns {
		if v.Comment == "" {
			columns = append(columns, map[string]interface{}{
				"name": v.Name,
			})
		} else {
			columns = append(columns, map[string]interface{}{
				"name":    v.Name,
				"comment": v.Comment,
			})
		}
	}
	return d.Set("column", columns)
}

func ReadView(c *exasol.Conn, schema, name string) (*View, error) {
	stmt := "SELECT VIEW_COMMENT, VIEW_TEXT FROM EXA_ALL_VIEWS WHERE UPPER(VIEW_SCHEMA) = UPPER(?) AND UPPER(VIEW_NAME) = UPPER(?)"
	res, err := c.FetchSlice(stmt, []interface{}{
		schema,
		name,
	}, "SYS")
	if err != nil {
		return nil, fmt.Errorf("selecting View Metadata for %s.%s failed: %s", schema, name, err)
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("selecting View Metadata for %s.%s resulted in no result%w", schema, name, ReadViewNoResultError)
	}

	row := res[0]
	comment, err := readViewComment(row)
	if err != nil {
		return nil, err
	}

	columns, err := readViewColumnsByRow(row)
	if err != nil {
		return nil, err
	}

	subquery, err := readViewSubquery(row)
	if err != nil {
		return nil, err
	}

	return &View{
		Comment:  comment,
		Columns:  columns,
		Subquery: subquery,
	}, nil
}

func readViewComment(row []interface{}) (string, error) {

	if row[0] == nil {
		return "", nil // No comment
	}

	return row[0].(string), nil
}

func scanComma(data []byte, atEOF bool) (advance int, token []byte, err error) {

	// Skip leading spaces.
	start := 0

	for width := 0; start < len(data); start += width {
		var r rune
		r, width = utf8.DecodeRune(data[start:])
		if !unicode.IsSpace(r) {
			break
		}
	}

	// Scan until comma, marking end of part.

	for width, i := 0, start; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		if r == rune(',') {
			return i + width, data[start:i], nil
		}
	}

	// If we're at EOF, we have a final, non-empty, non-terminated part. Return it.

	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}

	// Request more data.
	return start, nil, nil

}

func readViewColumnsByRow(row []interface{}) ([]ViewColumn, error) {
	text := row[1].(string)
	return readViewColumnsByString(text)
}

func readViewColumnsByString(text string) ([]ViewColumn, error) {
	submatch := columnReg.FindSubmatch([]byte(text))
	if len(submatch) == 0 {
		return nil, nil
	}
	if len(submatch) != 2 {
		return nil, fmt.Errorf("extracting columns failed: %s", text)
	}

	return parseColumnsString(string(submatch[1]))
}

func parseColumnsString(text string) ([]ViewColumn, error) {

	columnsText := text
	columns := []ViewColumn{}

	scanner := bufio.NewScanner(strings.NewReader(columnsText))
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = scanComma(data, atEOF)
		return
	}
	scanner.Split(split)
	for scanner.Scan() {
		columnText := scanner.Text()
		columns = append(columns, scanColumn(columnText))
	}

	return columns, nil
}

func scanColumn(text string) ViewColumn {

	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Split(bufio.ScanWords)
	scanner.Scan()
	columnName := scanner.Text()
	if !scanner.Scan() {
		return ViewColumn{
			Name: columnName,
		}
	}

	scanner.Scan()
	scanner.Scan()
	return ViewColumn{
		Name:    columnName,
		Comment: strings.Trim(scanner.Text(), "'"),
	}
}

func readViewSubquery(row []interface{}) (string, error) {
	text := row[1].(string)

	submatch := subqueryReg.FindSubmatch([]byte(text))
	if len(submatch) == 0 {
		return "", fmt.Errorf("regex matching Views CREATE text failed: %s", text)
	}
	if len(submatch) != 2 {
		return "", fmt.Errorf("extracting subquery failed: %s", text)
	}
	return string(submatch[1]), nil
}
