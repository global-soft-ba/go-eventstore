package validator

var searchForbiddenChars = []rune{';'}

// ValidateSearchString checks if the search string meets certain criteria
func ValidateSearchString(input string) bool {
	for _, char := range input {
		for _, forbiddenChar := range searchForbiddenChars {
			if char == forbiddenChar {
				return false
			}
		}
	}
	return true
}
