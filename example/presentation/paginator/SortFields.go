package paginator

type SortField struct {
	Name    string `json:"name"`
	FieldID string `json:"fieldID"`
	Desc    bool   `json:"desc"`
}

type SortFields []SortField

func (s SortFields) ToQueryParam() string {
	var result string
	for _, field := range s {
		if result != "" {
			result += ","
		}
		if field.Desc {
			result += "-"
		} else {
			result += "+"
		}
		result += field.Name
	}
	return result
}
