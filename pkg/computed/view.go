package computed

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/pkg/errors"
)

var (
	columnReg             = regexp.MustCompile(`(?s)CREATE\s+(?:OR\s+REPLACE|FORCE)?\s?VIEW\s+.*?\s+\((.*)\)\s+AS`)
	subqueryReg           = regexp.MustCompile(`(?s)CREATE\s+(?:OR\s+REPLACE|FORCE)?\s?VIEW\s+.*?AS\s+(.*?)(?:\s+COMMENT\s+IS.*)?$`)
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

func ReadView(ctx context.Context, tx *sql.Tx, schema, name string) (*View, error) {
	stmt := "SELECT VIEW_COMMENT, VIEW_TEXT FROM SYS.EXA_ALL_VIEWS WHERE UPPER(VIEW_SCHEMA) = UPPER(?) AND UPPER(VIEW_NAME) = UPPER(?)"
	res, err := tx.QueryContext(ctx, stmt, schema, name)
	if err != nil {
		return nil, fmt.Errorf("selecting View Metadata for %s.%s failed: %s", schema, name, err)
	}

	if !res.Next() {
		return nil, fmt.Errorf("selecting View Metadata for %s.%s resulted in no result%w", schema, name, ReadViewNoResultError)
	}

	var c interface{}
	var text string
	err = res.Scan(&c, &text)
	if err != nil {
		return nil, errors.Wrap(err, "View Query scan failed")
	}

	columns, err := readViewColumnsByString(text)
	if err != nil {
		return nil, err
	}

	subquery, err := readViewSubquery(text)
	if err != nil {
		return nil, err
	}

	var comment string
	if c == nil {
		comment = ""
	} else {
		comment = c.(string)
	}

	return &View{
		Comment:  comment,
		Columns:  columns,
		Subquery: subquery,
	}, nil
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

func readViewSubquery(text string) (string, error) {
	submatch := subqueryReg.FindSubmatch([]byte(text))
	if len(submatch) == 0 {
		return "", fmt.Errorf("regex matching Views CREATE text failed: %s", text)
	}
	if len(submatch) != 2 {
		return "", fmt.Errorf("extracting subquery failed: %s", text)
	}
	return string(submatch[1]), nil
}
