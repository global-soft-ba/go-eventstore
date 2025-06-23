//go:build unit

package paginator

import (
	"reflect"
	"testing"
)

func TestMapSortFields(t *testing.T) {
	type args struct {
		input             string
		defaultSortField  string
		defaultDescending bool
		mapping           map[string]string
	}
	tests := []struct {
		name           string
		args           args
		wantSortFields []SortField
	}{
		{
			name: "No SortField passed, fallback to defaults",
			args: args{
				input:             "",
				defaultSortField:  "SortID",
				defaultDescending: true,
				mapping:           nil,
			},
			wantSortFields: []SortField{
				{
					FieldID: "SortID",
					Desc:    true,
				},
			},
		},
		{
			name: "SortField not mapped, fallback to defaults",
			args: args{
				input:             "+ID",
				defaultSortField:  "SortID",
				defaultDescending: true,
				mapping:           nil,
			},
			wantSortFields: []SortField{
				{
					Name:    "id",
					FieldID: "SortID",
					Desc:    false,
				},
			},
		},
		{
			name: "Multiple case-insensitive SortFields passed and mapped",
			args: args{
				input:             "+id,-Name, AGE,+ Gender",
				defaultSortField:  "SortName",
				defaultDescending: true,
				mapping: map[string]string{
					"ID":     "SortID",
					"Name":   "SortName",
					"AGE":    "SortAge",
					"Gender": "SortGender",
				},
			},
			wantSortFields: []SortField{
				{
					Name:    "id",
					FieldID: "SortID",
					Desc:    false,
				},
				{
					Name:    "name",
					FieldID: "SortName",
					Desc:    true,
				},
				{
					Name:    "age",
					FieldID: "SortAge",
					Desc:    true,
				},
				{
					Name:    "gender",
					FieldID: "SortGender",
					Desc:    false,
				},
			},
		},
		{
			name: "Just ,",
			args: args{
				input:             ",",
				defaultSortField:  "SortID",
				defaultDescending: false,
				mapping:           nil,
			},
			wantSortFields: []SortField{
				{
					FieldID: "SortID",
					Desc:    false,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotSortFields := MapSortFields(tt.args.input, tt.args.defaultSortField, tt.args.defaultDescending, tt.args.mapping); !reflect.DeepEqual(gotSortFields, tt.wantSortFields) {
				t.Errorf("MapSortFields()\ngot:  %+v\nwant: %+v", gotSortFields, tt.wantSortFields)
			}
		})
	}
}
