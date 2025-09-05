//go:build unit

package validator

import (
	"testing"
)

func TestValidateSearchString(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"hello", true},
		{"hello;", false},
		{"hello world", true},
		{"hello;world", false},
	}

	for _, test := range tests {
		result := ValidateSearchString(test.input)
		if result != test.expected {
			t.Errorf("ValidateSearchString(%q) = %v; want %v", test.input, result, test.expected)
		}
	}
}
