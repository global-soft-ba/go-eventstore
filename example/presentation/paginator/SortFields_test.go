//go:build unit

package paginator

import "testing"

func TestSortFields_String(t *testing.T) {
	tests := []struct {
		name string
		s    SortFields
		want string
	}{
		{
			name: "Asc Desc Mix",
			s: []SortField{
				{
					FieldID: "SortColumn1",
					Name:    "col1",
					Desc:    false,
				},
				{
					FieldID: "SortColumn2",
					Name:    "col2",
					Desc:    true,
				},
			},
			want: "+col1,-col2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.ToQueryParam(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
