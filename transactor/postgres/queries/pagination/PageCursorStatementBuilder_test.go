//go:build unit

package pagination

import (
	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"testing"
)

func keySetPaginator(t *testing.T, size uint, idField string, sortFields []SortField, val []interface{}, backward bool) PageCursor {
	p, err := NewPageCursor(size, idField, sortFields, val, backward)
	if err != nil {
		t.Fatal(err)
	}

	return p
}

func TestCursorPagination(t *testing.T) {
	t.Parallel()
	type args struct {
		q      squirrel.SelectBuilder
		cursor PageCursor
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "nil cursor ",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: PageCursor{},
			},
			want: "SELECT a, b, c",
		},
		{
			name: "empty cursor",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 0, "id", nil, nil, false),
			},
			want: "SELECT a, b, c",
		},
		{
			name: "initial cursor (id only, no values)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", nil, nil, false),
			},
			want: "SELECT a, b, c ORDER BY id ASC FETCH FIRST 10 ROWS ONLY",
		},
		{
			name: "simple forward cursor (id excluded)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", nil, []interface{}{1}, false),
			},
			want: "SELECT a, b, c WHERE ( id > ? ) ORDER BY id ASC FETCH FIRST 10 ROWS ONLY",
		},
		{
			name: "simple forward cursor (id included)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"id", false}}, []interface{}{1}, false),
			},
			want: "SELECT a, b, c WHERE ( id > ? ) ORDER BY id ASC FETCH FIRST 10 ROWS ONLY",
		},
		{
			name: "simple backward cursor (id included)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"id", false}}, []interface{}{1}, true),
			},
			want: "SELECT * FROM (SELECT a, b, c WHERE ( id < ? ) ORDER BY id DESC FETCH FIRST 10 ROWS ONLY) AS tmp ORDER BY id ASC",
		},
		{
			name: "simple forward cursor descending (id included)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"id", true}}, []interface{}{1}, false),
			},
			want: "SELECT a, b, c WHERE ( id < ? ) ORDER BY id DESC FETCH FIRST 10 ROWS ONLY",
		},
		{
			name: "simple backward cursor descending (id excluded)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"id", true}}, []interface{}{1}, true),
			},
			want: "SELECT * FROM (SELECT a, b, c WHERE ( id > ? ) ORDER BY id ASC FETCH FIRST 10 ROWS ONLY) AS tmp ORDER BY id DESC",
		},
		{
			name: "multiple fields forward cursor (all ascending, excl. id)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"a", false}, {"b", false}}, []interface{}{1, 2, 3}, false),
			},
			want: "SELECT a, b, c WHERE ( a > ? OR ( a = ? AND b > ? ) OR ( a = ? AND b = ? AND id > ? ) ) ORDER BY a ASC, b ASC, id ASC FETCH FIRST 10 ROWS ONLY",
		},
		{
			name: "multiple fields backward cursor (all ascending, excl. id)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"a", false}, {"b", false}}, []interface{}{1, 2, 3}, true),
			},
			want: "SELECT * FROM (SELECT a, b, c WHERE ( a < ? OR ( a = ? AND b < ? ) OR ( a = ? AND b = ? AND id < ? ) ) ORDER BY a DESC, b DESC, id DESC FETCH FIRST 10 ROWS ONLY) AS tmp ORDER BY a ASC, b ASC, id ASC",
		},
		{
			name: "multiple fields forward cursor (all descending, excl. id)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"a", true}, {"b", true}}, []interface{}{1, 2, 3}, false),
			},
			want: "SELECT a, b, c WHERE ( a < ? OR ( a = ? AND b < ? ) OR ( a = ? AND b = ? AND id > ? ) ) ORDER BY a DESC, b DESC, id ASC FETCH FIRST 10 ROWS ONLY",
		},
		{
			name: "multiple fields backward cursor (all descending, excl. id)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"a", true}, {"b", true}}, []interface{}{1, 2, 3}, true),
			},
			want: "SELECT * FROM (SELECT a, b, c WHERE ( a > ? OR ( a = ? AND b > ? ) OR ( a = ? AND b = ? AND id < ? ) ) ORDER BY a ASC, b ASC, id DESC FETCH FIRST 10 ROWS ONLY) AS tmp ORDER BY a DESC, b DESC, id ASC",
		},

		{
			name: "multiple fields forward cursor (mixed asc and desc)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"a", true}, {"b", false}}, []interface{}{1, 2, 3}, false),
			},
			want: "SELECT a, b, c " +
				"WHERE ( a < ? OR ( a = ? AND b > ? ) OR ( a = ? AND b = ? AND id > ? ) ) " +
				"ORDER BY a DESC, b ASC, id ASC FETCH FIRST 10 ROWS ONLY",
		},
		{
			name: "multiple fields backward cursor (mixed asc and desc)",
			args: args{
				q:      squirrel.Select("a", "b", "c"),
				cursor: keySetPaginator(t, 10, "id", []SortField{{"a", true}, {"b", false}}, []interface{}{1, 2, 3}, true),
			},
			want: "SELECT * FROM (SELECT a, b, c " +
				"WHERE ( a > ? OR ( a = ? AND b < ? ) OR ( a = ? AND b = ? AND id < ? ) ) " +
				"ORDER BY a ASC, b DESC, id DESC FETCH FIRST 10 ROWS ONLY) AS tmp ORDER BY a DESC, b ASC, id ASC",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := CursorPagination(squirrel.Question, tt.args.q, tt.args.cursor).ToSql()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)

		})
	}
}
