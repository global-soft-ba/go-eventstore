package pagination

import (
	"fmt"
)

// IKeySetPagination represents a page retrieval of data starting by the LastToken (not included) until the given page size.
// We are using a cursor-based pagination. Keyset pagination is a method for efficiently navigating large datasets in a database.
// Instead of specifying offsets and limits, it uses a cursor, i.e. set of fields, typically including a unique identifier,
// to indicate the next record to fetch. This approach offers better performance and consistency,
// making it ideal for handling substantial amounts of data.
//
// One disadvantage of cursor-based pagination is that it might not be as straightforward to jump to specific pages
// (random page access) within the dataset compared to offset-based pagination.
//
//	Additionally, keyset-based pagination may assert more complex implementation when compared to the simpler offset and limit approach.
//
// SQL-EXAMPLE:
// SELECT * FROM books
// Where (id > '5')
//	  OR (id = '5' AND created_at < '1974-11')
//
// ORDER BY id, created_at DESC
// FETCH FIRST 1 ROWS ONLY;
//
//
// Reference:
//   * https://medium.com/@george_16060/cursor-based-pagination-with-arbitrary-ordering-b4af6d5e22db
//   * https://dev.to/tariqabughofa/how-to-paginate-the-right-way-in-sql-hdc
//   * https://www.citusdata.com/blog/2016/03/30/five-ways-to-paginate/
//   * https://www.cybertec-postgresql.com/en/pagination-problem-total-result-count/
//   * https://use-the-index-luke.com/no-offset

type SortField struct {
	Name string
	Desc bool
}

func NewPageCursor(pageSize uint, idFieldName string, sortFields []SortField, val []interface{}, backward bool) (PageCursor, error) {
	// idField the identifier columns that guarantee uniqueness and enables the correct assignment of last token values to
	// the corresponding table columns. It must be always specified but not always contained in the sortFields.
	// If it is not contained in the sortFields, it will be added automatically.

	pageCursor := PageCursor{
		sortFields: sortFields,
		values:     val,
		size:       pageSize,
		backward:   backward,
	}

	pageCursor = insertIDFieldInSortOrderIfNeeded(pageCursor, idFieldName)
	if val != nil && (len(pageCursor.SortFields()) != len(val)) {
		return PageCursor{}, fmt.Errorf("fields and values must have the same length")
	}

	return pageCursor, nil
}

type PageCursor struct {
	// sortFields represents the columns to sort the result by (including ID column).
	sortFields []SortField

	// values represent a bookmark of the last element in the previous page.
	// It's a slice of interface{} because it can contain any data type.
	values []interface{}

	//PageSize represents the number of elements to retrieve.
	size uint

	//cursor can be executed in a forward and backward manner; false = forward, true = backward
	backward bool
}

func (p PageCursor) IsEmpty() bool {
	return p.size == 0
}

func (p PageCursor) HasSortFields() bool {
	return len(p.sortFields) > 0
}

func (p PageCursor) PageSize() uint {
	return p.size
}

func (p PageCursor) Backward() bool {
	return p.backward
}

func (p PageCursor) SortFields() []SortField {
	return p.sortFields
}

func (p PageCursor) Values() []interface{} {
	return p.values
}

//----------------------------------------------helper function--------------------------------------------

func insertIDFieldInSortOrderIfNeeded(p PageCursor, idFieldName string) PageCursor {
	var sortFieldsWithID []SortField

	switch {
	case iDFieldContainedInSortFields(p.sortFields, idFieldName):
		return p
	default:
		sortFieldsWithID = append(p.sortFields, SortField{
			Name: idFieldName,
			Desc: false,
		})
		p.sortFields = sortFieldsWithID
		return p
	}
}

func iDFieldContainedInSortFields(sortFields []SortField, fieldName string) bool {
	for _, field := range sortFields {
		if field.Name == fieldName {
			return true
		}
	}
	return false
}
