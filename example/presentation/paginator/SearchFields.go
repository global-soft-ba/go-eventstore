package paginator

type SearchOperator string

const (
	SearchGreaterThan        SearchOperator = ">"
	SearchGreaterThanOrEqual SearchOperator = ">="
	SearchLessThan           SearchOperator = "<"
	SearchLessThanOrEqual    SearchOperator = "<="
	SearchEqual              SearchOperator = "="
	SearchNotEqual           SearchOperator = "!="
	SearchMatch              SearchOperator = "~*"
	SearchEqualAlias         string         = ":"
)

type SearchField struct {
	Name     string         `json:"name"`
	FieldID  string         `json:"fieldID"`
	Value    string         `json:"value"`
	Operator SearchOperator `json:"comparison"`
}

type SearchFields []SearchField

func (s SearchFields) ToQueryParam() string {
	var result string
	for _, field := range s {
		if result != "" {
			result += " "
		}
		if field.Name == "" {
			result += field.Value
		} else {
			if field.Operator == SearchEqual {
				result += "@" + field.Name + SearchEqualAlias + field.Value
			} else {
				result += "@" + field.Name + string(field.Operator) + field.Value
			}
		}

	}
	return result
}
