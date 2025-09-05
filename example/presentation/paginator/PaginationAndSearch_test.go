//go:build unit

package paginator

import (
	"example/presentation/test"
	"github.com/gin-gonic/gin"
	"reflect"
	"testing"
)

func TestParsePaginationAndSearch(t *testing.T) {
	type args struct {
		ctx              *gin.Context
		sortMapping      map[string]string
		searchMapping    map[string]string
		defaultSortField string
		maxSize          uint
	}
	tests := []struct {
		name    string
		args    args
		want    PageDTO
		wantErr bool
	}{
		{
			name: "All params set without values",
			args: args{
				ctx:              test.GetTestContext("size=10&sort=field1,-field2&search=@field1:value1 @field2:value2"),
				sortMapping:      map[string]string{"field1": "SortField1", "field2": "SortField2"},
				searchMapping:    map[string]string{"field1": "SearchField1", "field2": "SearchField2"},
				defaultSortField: "SearchField1",
				maxSize:          1000,
			},
			want: PageDTO{
				PageSize: 10,
				SortFields: []SortField{
					{Name: "field1", FieldID: "SortField1", Desc: false},
					{Name: "field2", FieldID: "SortField2", Desc: true},
				},
				SearchFields: []SearchField{
					{Name: "field1", FieldID: "SearchField1", Value: "value1", Operator: SearchEqual},
					{Name: "field2", FieldID: "SearchField2", Value: "value2", Operator: SearchEqual},
				},
				Values:     nil,
				IsBackward: false,
			},
			wantErr: false,
		},
		{
			name: "All params set with values",
			args: args{
				ctx:              test.GetTestContext("size=10&sort=field1,-field2&search=@field1:value1 @field2:value2&values=1000&values=Max&values=Mustermann"),
				sortMapping:      map[string]string{"field1": "SortField1", "field2": "SortField2"},
				searchMapping:    map[string]string{"field1": "SearchField1", "field2": "SearchField2"},
				defaultSortField: "SearchField1",
				maxSize:          1000,
			},
			want: PageDTO{
				PageSize: 10,
				SortFields: []SortField{
					{Name: "field1", FieldID: "SortField1", Desc: false},
					{Name: "field2", FieldID: "SortField2", Desc: true},
				},
				SearchFields: []SearchField{
					{Name: "field1", FieldID: "SearchField1", Value: "value1", Operator: SearchEqual},
					{Name: "field2", FieldID: "SearchField2", Value: "value2", Operator: SearchEqual},
				},
				Values:     []interface{}{"1000", "Max", "Mustermann"},
				IsBackward: false,
			},
			wantErr: false,
		},
		{
			name: "No params set - using defaults",
			args: args{
				ctx:              test.GetTestContext(""),
				sortMapping:      map[string]string{"field1": "SortField1", "field2": "SortField2"},
				searchMapping:    map[string]string{"field1": "SearchField1", "field2": "SearchField2"},
				defaultSortField: "SortField1",
				maxSize:          1000,
			},
			want: PageDTO{
				PageSize: 1000,
				SortFields: []SortField{
					{Name: "", FieldID: "SortField1", Desc: false},
				},
				SearchFields: []SearchField{},
				Values:       nil,
				IsBackward:   false,
			},
			wantErr: false,
		},
		{
			name: "Invalid search field",
			args: args{
				ctx:              test.GetTestContext("size=10&sort=field1,-field2&search=%3B@field1:value1 @field2:value2"),
				sortMapping:      map[string]string{"field1": "SortField1", "field2": "SortField2"},
				searchMapping:    map[string]string{"field1": "SearchField1", "field2": "SearchField2"},
				defaultSortField: "SearchField1",
				maxSize:          1000,
			},
			want:    PageDTO{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePaginationAndSearch(tt.args.ctx, tt.args.sortMapping, tt.args.searchMapping, tt.args.defaultSortField, tt.args.maxSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePaginationAndSearch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParsePaginationAndSearch()\ngot:  %+v\nwant: %+v", got, tt.want)
			}
		})
	}
}
