package internal

type Data interface {
	Get(name string) interface{}
	Set(name string, d interface{}) error
	Id() string
	SetId(id string)
}
