package internal

type Conn interface {
	FetchSlice(string, ...interface{}) ([][]interface{}, error)
}
