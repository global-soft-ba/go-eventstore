package paginator

import (
	"example/presentation/extractor"
	"example/presentation/validator"
	"fmt"
	"github.com/gin-gonic/gin"
)

const SearchAny string = "any"

type PagesDTO struct {
	Previous PageDTO `json:"previous"`
	Next     PageDTO `json:"next"`
}

type PageDTO struct {
	PageSize     uint          `json:"pageSize"`
	SortFields   SortFields    `json:"sortFields"`
	SearchFields SearchFields  `json:"searchFields"`
	Values       []interface{} `json:"values"`
	IsBackward   bool          `json:"isBackward"`
}

const (
	QueryParamSize      = "size"
	QueryParamSort      = "sort"
	QueryParamSearch    = "search"
	QueryParamValues    = "values"
	QueryParamDirection = "direction"
)

func ParsePaginationAndSearch(ctx *gin.Context, sortMapping, searchMapping map[string]string, defaultSortField string, maxPageSize uint) (PageDTO, error) {
	pageSize := extractor.ExtractUintValueWithMinMax(ctx, QueryParamSize, maxPageSize, 1, maxPageSize)
	sortTerm := ctx.Query(QueryParamSort)
	sortFields := MapSortFields(sortTerm, defaultSortField, false, sortMapping)
	searchTerm := ctx.Query(QueryParamSearch)
	if !validator.ValidateSearchString(searchTerm) {
		return PageDTO{}, fmt.Errorf("could not parse pagination and search: invalid search term %q", searchTerm)
	}
	searchFields := MapSearchFields(searchTerm, searchMapping)
	values := ToInterfaceSlice(ctx.QueryArray(QueryParamValues))
	pageDirection := extractor.ExtractBool(ctx, QueryParamDirection, false)
	return PageDTO{
			PageSize:     pageSize,
			SortFields:   sortFields,
			SearchFields: searchFields,
			Values:       values,
			IsBackward:   pageDirection,
		},
		nil
}
