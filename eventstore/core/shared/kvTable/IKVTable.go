package kvTable

import (
	"errors"
	"fmt"
)

type KeyNotFoundError struct {
	key Key
}

func (c *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key %v not found in table", c.key)
}

type IKVTable[V any] interface {
	get(key Key) ([]V, error)
	getPartial(key Key) ([]V, error)
	set(key Key, values ...V) error
	add(key Key, values ...V) error
	del(key Key) error

	//used on the []V per key3
	union(table IKVTable[V]) error
	intersect(table IKVTable[V]) error
	replace(table IKVTable[V]) error

	copy() IKVTable[V]

	tableDataAsKeyValueMap() map[Key][]V
	tableDataAsSlice() []V
	count() int
}

func IsKeyNotFound(err error) bool {
	var keyNotFoundError *KeyNotFoundError
	return errors.As(err, &keyNotFoundError)
}

func Get[S any](table IKVTable[S], key Key) (empty []S, err error) {
	got, err := table.get(key)
	if err != nil {
		return empty, &KeyNotFoundError{key: key}
	}

	if len(got) == 0 {
		return empty, nil
	}

	return got, err
}

func GetFirst[S any](table IKVTable[S], key Key) (get S, err error) {
	got, err := Get(table, key)
	if err != nil {
		return get, err
	}

	if len(got) == 0 {
		return get, nil
	}

	return got[0], nil
}

func GetPartial[S any](table IKVTable[S], key Key) (get []S, err error) {
	return table.getPartial(key)
}

func Set[S any](table IKVTable[S], key Key, values ...S) (err error) {
	return table.set(key, values...)
}

func Add[S any](table IKVTable[S], key Key, values ...S) (err error) {
	return table.add(key, values...)
}

func Del[S any](table IKVTable[S], key Key) (err error) {
	return table.del(key)
}

func Union[S any](table1, table2 IKVTable[S]) error {
	return table1.union(table2)
}

func Intersect[S any](table1, table2 IKVTable[S]) error {
	return table1.intersect(table2)
}

func Replace[S any](table1, table2 IKVTable[S]) error {
	return table1.replace(table2)
}

func Copy[S any](table IKVTable[S]) IKVTable[S] {
	return table.copy()
}

func TableDataAsKeyValueMap[S any](table IKVTable[S]) map[Key][]S {
	return table.tableDataAsKeyValueMap()
}

func DataAsSlice[S any](table IKVTable[S]) []S {
	return table.tableDataAsSlice()
}

func Count[S any](table IKVTable[S]) int {
	return table.count()
}
