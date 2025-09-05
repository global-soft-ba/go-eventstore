//go:build unit

package sqExtend

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArrayAppend(t *testing.T) {
	type args struct {
		in  sq.Sqlizer
		out sq.Sqlizer
	}
	tests := []struct {
		name     string
		args     args
		wantStmt string
		wantVal  []interface{}
	}{
		{
			name: "test single",
			args: args{
				in:  sq.Expr("?", 1),
				out: sq.Expr("?", 2),
			},
			wantStmt: "ARRAY_APPEND(?,?)",
			wantVal:  []interface{}{1, 2},
		},
		{
			name: "test in combination with coalesce",
			args: args{
				in:  Coalesce(sq.Expr("?", "col"), sq.Expr("?", "default")),
				out: sq.Expr("?", 2),
			},
			wantStmt: "ARRAY_APPEND(COALESCE(?,?),?)",
			wantVal:  []interface{}{"col", "default", 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, val, err := ArrayAppend(tt.args.in, tt.args.out).ToSql()
			assert.Nil(t, err)
			assert.Equal(t, tt.wantStmt, stmt)
			assert.Equal(t, tt.wantVal, val)

		})
	}
}
