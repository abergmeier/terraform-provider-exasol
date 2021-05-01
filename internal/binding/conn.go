package binding

type Conn interface {
	Commit() error
	Execute(sql string, args ...interface{}) (rowsAffected int64, err error)
	FetchSlice(sql string, args ...interface{}) (res [][]interface{}, err error)
}
