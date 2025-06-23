package kvTable

type Key interface {
	partialMatch(searchKey Key) bool
}

const KeyWildcardString = "*"

//unfortunately the expression "arbitrary arrays of a given type" is currently not supported by the generic implementation
//as soon as this works, the duplication to type and the switch block can be omitted

//https://go.dev/ref/spec#Slice_types
//Conversions from slice to array pointer
//
//Converting a slice to an array pointer yields a pointer to the underlying array of the slice. If the length of the slice is less than the length of the array, a run-time panic occurs.
//
//s := make([]byte, 2, 4)
//s0 := (*[0]byte)(s)      // s0 != nil
//s1 := (*[1]byte)(s[1:])  // &s1[0] == &s[1]
//s2 := (*[2]byte)(s)      // &s2[0] == &s[0]
//s4 := (*[4]byte)(s)      // panics: len([4]byte) > len(s)
//
//var t []string
//t0 := (*[0]string)(t)    // t0 == nil
//t1 := (*[1]string)(t)    // panics: len([1]string) > len(t)
//
//u := make([]byte, 0)
//u0 := (*[0]byte)(u)      // u0 != nil

func NewKey(keyParts ...string) Key {

	switch len(keyParts) {
	case 1:
		return stringKey1{keyParts: *(*[1]string)(keyParts)}
	case 2:
		return stringKey2{keyParts: *(*[2]string)(keyParts)}
	case 3:
		return stringKey3{keyParts: *(*[3]string)(keyParts)}
	case 4:
		return stringKey4{keyParts: *(*[4]string)(keyParts)}
	}

	return nil
}

type stringKey1 struct {
	keyParts [1]string
}

func (k stringKey1) partialMatch(searchKey Key) bool {
	search, ok := searchKey.(stringKey1)
	if !ok {
		return false
	}

	for i, keyPart := range k.keyParts {
		if !(keyPart == search.keyParts[i] || search.keyParts[i] == KeyWildcardString) {
			return false
		}
	}
	return true
}

type stringKey2 struct {
	keyParts [2]string
}

func (k stringKey2) partialMatch(searchKey Key) bool {
	search, ok := searchKey.(stringKey2)
	if !ok {
		return false
	}

	for i, keyPart := range k.keyParts {
		if !(keyPart == search.keyParts[i] || search.keyParts[i] == KeyWildcardString) {
			return false
		}
	}
	return true
}

type stringKey3 struct {
	keyParts [3]string
}

func (k stringKey3) partialMatch(searchKey Key) bool {
	search, ok := searchKey.(stringKey3)
	if !ok {
		return false
	}

	for i, keyPart := range k.keyParts {
		if !(keyPart == search.keyParts[i] || search.keyParts[i] == KeyWildcardString) {
			return false
		}
	}
	return true
}

type stringKey4 struct {
	keyParts [4]string
}

func (k stringKey4) partialMatch(searchKey Key) bool {
	search, ok := searchKey.(stringKey4)
	if !ok {
		return false
	}

	for i, keyPart := range k.keyParts {
		if !(keyPart == search.keyParts[i] || search.keyParts[i] == KeyWildcardString) {
			return false
		}
	}
	return true
}
