package kvTable

import (
	"fmt"
	"reflect"
	"sync"
)

func NewKeyValuesTable[S any]() IKVTable[S] {
	return &kVTable[S]{
		tableData: make(map[Key][]S),
	}
}

type kVTable[V any] struct {
	sync.RWMutex
	tableData map[Key][]V
}

func (s *kVTable[V]) get(key Key) (result []V, err error) {
	s.RLock()
	result, ok := s.tableData[key]
	s.RUnlock()
	if !ok {
		return result, fmt.Errorf("key3 %v not found", key)
	}

	return result, err
}

func (s *kVTable[V]) getPartial(key Key) (result []V, err error) {
	s.RLock()
	defer s.RUnlock()

	for k, val := range s.tableData {
		if k.partialMatch(key) {
			result = append(result, val...)
		}
	}

	return result, err
}

func (s *kVTable[V]) set(key Key, values ...V) error {
	s.Lock()
	s.tableData[key] = values
	s.Unlock()

	return nil
}

func (s *kVTable[V]) add(key Key, values ...V) error {
	s.Lock()
	s.tableData[key] = append(s.tableData[key], values...)
	s.Unlock()

	return nil
}

// does not respect key3 wildcards
func (s *kVTable[V]) del(key Key) error {
	s.Lock()
	delete(s.tableData, key)
	s.Unlock()

	return nil
}

func (s *kVTable[V]) tableDataAsKeyValueMap() map[Key][]V {
	s.RLock()
	defer s.RUnlock()

	return s.tableData
}

func (s *kVTable[V]) tableDataAsSlice() (result []V) {
	for _, agg := range s.tableData {
		result = append(result, agg...)
	}

	return result
}

func (s *kVTable[V]) union(table IKVTable[V]) error {
	s.Lock()
	defer s.Unlock()

	for k, v := range table.tableDataAsKeyValueMap() {
		s.tableData[k] = append(s.tableData[k], v...)
	}
	return nil
}

func (s *kVTable[V]) intersect(table IKVTable[V]) error {
	s.Lock()
	defer s.Unlock()

	//for all keys
	for k, vals2 := range table.tableDataAsKeyValueMap() {
		if vals1, err := s.tableData[k]; !err {
			s.tableData[k] = s.intersectValues(vals1, vals2)
		}
	}
	return nil
}

func (s *kVTable[V]) intersectValues(a, b []V) []V {
	var result []V
	for _, v1 := range a {
		for _, v2 := range b {
			if reflect.DeepEqual(v1, v2) {
				result = append(result, v1)
			}
		}
	}
	return result
}

func (s *kVTable[V]) replace(table IKVTable[V]) error {
	s.Lock()
	defer s.Unlock()

	//for all keys
	for k, val := range table.tableDataAsKeyValueMap() {
		if val != nil {
			s.tableData[k] = val
		} else {
			delete(s.tableData, k)
		}
	}
	return nil
}

func (s *kVTable[V]) copy() IKVTable[V] {
	s.Lock()
	defer s.Unlock()
	clone := make(map[Key][]V)

	for k, v := range s.tableData {
		var data []V
		for _, v2 := range v {
			data = append(data, v2)
		}
		clone[k] = data
	}

	return &kVTable[V]{
		tableData: clone,
	}

}

func (s *kVTable[V]) count() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.tableData)
}
