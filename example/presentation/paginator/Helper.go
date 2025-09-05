package paginator

import "strings"

func toCaseInsensitiveMapping(mapping map[string]string) (result map[string]string) {
	result = make(map[string]string, len(mapping))
	for key, value := range mapping {
		result[strings.ToLower(key)] = value
	}
	return result
}

func ToInterfaceSlice(input []string) []interface{} {
	var values []interface{}
	for _, value := range input {
		values = append(values, value)
	}
	return values
}
