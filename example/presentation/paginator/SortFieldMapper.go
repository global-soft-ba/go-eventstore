package paginator

import "strings"

func MapSortFields(input, defaultSortField string, defaultDescending bool, mapping map[string]string) (sortFields []SortField) {
	mapping = toCaseInsensitiveMapping(mapping)
	sortFields = extractSortingFields(input, defaultDescending)
	for i, field := range sortFields {
		if fieldID, ok := mapping[field.Name]; ok {
			field.FieldID = fieldID
		} else {
			field.FieldID = defaultSortField
		}
		sortFields[i] = field
	}
	if len(sortFields) == 0 {
		sortFields = append(sortFields, SortField{
			FieldID: defaultSortField,
			Desc:    defaultDescending,
		})
	}
	return sortFields
}

func extractSortingFields(fields string, defaultDescending bool) (rawSortFields []SortField) {
	for _, field := range strings.Split(fields, ",") {
		field = strings.ToLower(strings.TrimSpace(field))
		if len(field) == 0 {
			continue
		}
		switch {
		case strings.HasPrefix(field, "-"):
			rawSortFields = append(rawSortFields, SortField{
				Name: strings.TrimSpace(strings.TrimPrefix(field, "-")),
				Desc: true,
			})
		case strings.HasPrefix(field, "+"):
			rawSortFields = append(rawSortFields, SortField{
				Name: strings.TrimSpace(strings.TrimPrefix(field, "+")),
				Desc: false,
			})
		default:
			rawSortFields = append(rawSortFields, SortField{
				Name: field,
				Desc: defaultDescending,
			})
		}
	}
	return rawSortFields
}
