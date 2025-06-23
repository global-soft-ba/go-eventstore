//go:build unit

package paginator

import "testing"

func TestSearchFields_String(t *testing.T) {
	tests := []struct {
		name string
		s    SearchFields
		want string
	}{
		{
			name: "Mix of operators and fields",
			s: []SearchField{
				{
					Name:     SearchAny,
					FieldID:  SearchAny,
					Value:    "Something",
					Operator: SearchEqual,
				},
				{
					Name:     "id",
					FieldID:  "SearchID",
					Value:    "123",
					Operator: SearchGreaterThanOrEqual,
				},
			},
			want: "@any:Something @id>=123",
		},
		{
			name: "Empty field",
			s: []SearchField{
				{
					Name:     "",
					FieldID:  SearchAny,
					Value:    "Something",
					Operator: SearchEqual,
				},
			},
			want: "Something",
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
