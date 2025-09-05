package response

type PageDTO struct {
	Search   string
	Sort     string
	Size     uint
	Values   []interface{}
	Backward bool
}
