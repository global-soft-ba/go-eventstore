package paginator

import "strings"

func MapSearchFields(input string, mapping map[string]string) (searchFields []SearchField) {
	mapping = toCaseInsensitiveMapping(mapping)
	searchFields = extractSearchToken(input)
	for i, searchField := range searchFields {
		if searchFieldID, ok := mapping[searchField.Name]; ok {
			searchField.FieldID = searchFieldID
		} else {
			searchField.FieldID = SearchAny
		}
		searchFields[i] = searchField
	}
	return searchFields
}

func extractSearchToken(searchToken string) []SearchField {
	if len(strings.TrimSpace(searchToken)) == 0 {
		return []SearchField{}
	} else if !strings.Contains(searchToken, "@") {
		return []SearchField{
			{
				Name:     SearchAny,
				Value:    strings.TrimSpace(searchToken),
				Operator: SearchEqual,
			},
		}
	}
	parts := strings.Split(searchToken, " @")
	var searchFields []SearchField
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		split, op := splitByOperator(part)
		if len(split) != 2 {
			searchFields = append(searchFields, SearchField{
				Name:     SearchAny,
				Value:    strings.TrimSpace(split[0]),
				Operator: op,
			})
		} else {
			searchFields = append(searchFields, SearchField{
				Name:     strings.ToLower(strings.TrimPrefix(strings.TrimSpace(split[0]), "@")),
				Value:    strings.TrimSpace(split[1]),
				Operator: op,
			})
		}
	}
	return searchFields
}

func splitByOperator(searchToken string) (splitToken []string, op SearchOperator) {
	switch {
	//Two characters first
	case strings.Contains(searchToken, string(SearchGreaterThanOrEqual)):
		return strings.SplitN(searchToken, string(SearchGreaterThanOrEqual), 2), SearchGreaterThanOrEqual
	case strings.Contains(searchToken, string(SearchLessThanOrEqual)):
		return strings.SplitN(searchToken, string(SearchLessThanOrEqual), 2), SearchLessThanOrEqual
	case strings.Contains(searchToken, string(SearchNotEqual)):
		return strings.SplitN(searchToken, string(SearchNotEqual), 2), SearchNotEqual
	case strings.Contains(searchToken, string(SearchMatch)):
		return strings.SplitN(searchToken, string(SearchMatch), 2), SearchMatch
	//One character
	case strings.Contains(searchToken, string(SearchGreaterThan)):
		return strings.SplitN(searchToken, string(SearchGreaterThan), 2), SearchGreaterThan
	case strings.Contains(searchToken, string(SearchLessThan)):
		return strings.SplitN(searchToken, string(SearchLessThan), 2), SearchLessThan
	case strings.Contains(searchToken, SearchEqualAlias):
		return strings.SplitN(searchToken, SearchEqualAlias, 2), SearchEqual
	case strings.Contains(searchToken, string(SearchEqual)):
		fallthrough
	default:
		return strings.SplitN(searchToken, string(SearchEqual), 2), SearchEqual
	}
}
