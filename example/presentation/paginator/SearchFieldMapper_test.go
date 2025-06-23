//go:build unit

package paginator

import (
	"reflect"
	"testing"
)

func TestMapSearchFields(t *testing.T) {
	type args struct {
		input   string
		mapping map[string]string
	}
	tests := []struct {
		name             string
		args             args
		wantSearchFields []SearchField
	}{
		{
			name: "Search String doesn't contain tokens",
			args: args{
				input:   "Hello World",
				mapping: map[string]string{},
			},
			wantSearchFields: []SearchField{
				{
					Name:     SearchAny,
					FieldID:  SearchAny,
					Value:    "Hello World",
					Operator: SearchEqual,
				},
			},
		},
		{
			name: "Single Column that is mapped",
			args: args{
				input: "@Column:Value",
				mapping: map[string]string{
					"Column": "col",
				},
			},
			wantSearchFields: []SearchField{
				{
					Name:     "column",
					FieldID:  "col",
					Value:    "Value",
					Operator: SearchEqual,
				},
			},
		},
		{
			name: "Multiple case-insensitive Columns with different operators",
			args: args{
				input:   "@Column>=Value1 @column2:Value2 @COLUMN3~*123 @Column4<=Value4 @Column5!=Value5 @cOlumN6=Value6 @Column7>Value7 @Column8<Value8",
				mapping: map[string]string{"Column": "col", "Column2": "col2", "Column3": "col3", "Column4": "col4", "Column5": "col5", "Column6": "col6", "Column7": "col7", "Column8": "col8"},
			},
			wantSearchFields: []SearchField{
				{
					Name:     "column",
					FieldID:  "col",
					Value:    "Value1",
					Operator: SearchGreaterThanOrEqual,
				},
				{
					Name:     "column2",
					FieldID:  "col2",
					Value:    "Value2",
					Operator: SearchEqual,
				},
				{
					Name:     "column3",
					FieldID:  "col3",
					Value:    "123",
					Operator: SearchMatch,
				},
				{
					Name:     "column4",
					FieldID:  "col4",
					Value:    "Value4",
					Operator: SearchLessThanOrEqual,
				},
				{
					Name:     "column5",
					FieldID:  "col5",
					Value:    "Value5",
					Operator: SearchNotEqual,
				},
				{
					Name:     "column6",
					FieldID:  "col6",
					Value:    "Value6",
					Operator: SearchEqual,
				},
				{
					Name:     "column7",
					FieldID:  "col7",
					Value:    "Value7",
					Operator: SearchGreaterThan,
				},
				{
					Name:     "column8",
					FieldID:  "col8",
					Value:    "Value8",
					Operator: SearchLessThan,
				},
			},
		},
		{
			name: "Column name but no value",
			args: args{
				input:   "@Column",
				mapping: map[string]string{"Column": "col"},
			},
			wantSearchFields: []SearchField{
				{
					Name:     SearchAny,
					FieldID:  SearchAny,
					Value:    "@Column",
					Operator: SearchEqual,
				},
			},
		},
		{
			name: "@ in Search Value",
			args: args{
				input:   "@Email:office@test.globalsoft.com",
				mapping: map[string]string{"Email": "SearchFieldEmail"},
			},
			wantSearchFields: []SearchField{
				{
					Name:     "email",
					FieldID:  "SearchFieldEmail",
					Value:    "office@test.globalsoft.com",
					Operator: SearchEqual,
				},
			},
		},
		{
			name: "operator in Search Value",
			args: args{
				input:   "@UserID~*Some=User>ID",
				mapping: map[string]string{"UserID": "SearchFieldUserID"},
			},
			wantSearchFields: []SearchField{
				{
					Name:     "userid",
					FieldID:  "SearchFieldUserID",
					Value:    "Some=User>ID",
					Operator: SearchMatch,
				},
			},
		},
		{
			name: "trim spaces",
			args: args{
				input:   "@Column: value  @Column2:  value2 ",
				mapping: map[string]string{"Column": "col", "Column2": "col2"},
			},
			wantSearchFields: []SearchField{
				{
					Name:     "column",
					FieldID:  "col",
					Value:    "value",
					Operator: SearchEqual,
				},
				{
					Name:     "column2",
					FieldID:  "col2",
					Value:    "value2",
					Operator: SearchEqual,
				},
			},
		},
		{
			name: "@ as first character in search value",
			args: args{
				input:   "@Email:@globalsoft.com",
				mapping: map[string]string{"Email": "SearchFieldEmail"},
			},
			wantSearchFields: []SearchField{
				{
					Name:     "email",
					FieldID:  "SearchFieldEmail",
					Value:    "@globalsoft.com",
					Operator: SearchEqual,
				},
			},
		},
		{
			name: "space + @ as first character in search value gives weird behavior but we live with it",
			args: args{
				input:   "@Email: @globalsoft.com",
				mapping: map[string]string{"Email": "SearchFieldEmail"},
			},
			wantSearchFields: []SearchField{
				{
					Name:     "email",
					FieldID:  "SearchFieldEmail",
					Value:    "",
					Operator: SearchEqual,
				},
				{
					Name:     SearchAny,
					FieldID:  SearchAny,
					Value:    "globalsoft.com",
					Operator: SearchEqual,
				},
			},
		},
		{
			name: "Just space + @",
			args: args{
				input:   " @",
				mapping: nil,
			},
			wantSearchFields: nil,
		},
		{
			name: "Multiple space + @",
			args: args{
				input:   " @ @ @ @",
				mapping: nil,
			},
			wantSearchFields: nil,
		},
		{
			name: "Just @",
			args: args{
				input:   "@",
				mapping: nil,
			},
			wantSearchFields: []SearchField{
				{
					Name:     SearchAny,
					FieldID:  SearchAny,
					Value:    "@",
					Operator: SearchEqual,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotSearchFields := MapSearchFields(tt.args.input, tt.args.mapping); !reflect.DeepEqual(gotSearchFields, tt.wantSearchFields) {
				t.Errorf("MapSearchFields()\ngot:  %+v\nwant: %+v", gotSearchFields, tt.wantSearchFields)
			}
		})
	}
}
