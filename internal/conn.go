package internal

type Conn interface {
	Commit() error
	Execute(string, ...interface{}) (map[string]interface{}, error)
	FetchSlice(string, ...interface{}) ([][]interface{}, error)
}
