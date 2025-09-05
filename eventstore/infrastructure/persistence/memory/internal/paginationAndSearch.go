package internal

import (
	"github.com/global-soft-ba/go-eventstore"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func filterEvents(events []event.PersistenceEvent, searchFields []event.SearchField) []event.PersistenceEvent {
	var filteredEvents []event.PersistenceEvent

	for _, evt := range events {
		match := true
		for _, searchField := range searchFields {
			if !matchesSearchField(evt, searchField) {
				match = false
				break
			}
		}
		if match {
			filteredEvents = append(filteredEvents, evt)
		}
	}

	return filteredEvents
}

func matchesSearchField(evt event.PersistenceEvent, searchField event.SearchField) bool {
	switch searchField.Name {
	case event.SearchAggregateEventID:
		return compareString(evt.ID, searchField.Value, searchField.Operator)
	case event.SearchAggregateID:
		return compareString(evt.AggregateID, searchField.Value, searchField.Operator)
	case event.SearchAggregateType:
		return compareString(evt.AggregateType, searchField.Value, searchField.Operator)
	case event.SearchAggregateVersion:
		return compareInt(evt.Version, searchField.Value, searchField.Operator)
	case event.SearchAggregateEventType:
		return compareString(evt.Type, searchField.Value, searchField.Operator)
	case event.SearchAggregateClass:
		return compareString(string(evt.Class), searchField.Value, searchField.Operator)
	case event.SearchTransactionTime:
		return compareTime(evt.TransactionTime, searchField.Value, searchField.Operator)
	case event.SearchValidTime:
		return compareTime(evt.ValidTime, searchField.Value, searchField.Operator)
	case event.SearchData:
		return compareString(string(evt.Data), searchField.Value, searchField.Operator)
	default:
		return false
	}
}

func compareString(fieldValue string, searchValue string, operator event.SearchOperator) bool {
	switch operator {
	case event.SearchEqual:
		return fieldValue == searchValue
	case event.SearchNotEqual:
		return fieldValue != searchValue
	case event.SearchGreaterThan:
		return strings.Compare(fieldValue, searchValue) > 0
	case event.SearchGreaterThanOrEqual:
		return strings.Compare(fieldValue, searchValue) >= 0
	case event.SearchLessThan:
		return strings.Compare(fieldValue, searchValue) < 0
	case event.SearchLessThanOrEqual:
		return strings.Compare(fieldValue, searchValue) <= 0
	case event.SearchMatch:
		expr := regexp.MustCompilePOSIX(searchValue)
		return expr.MatchString(fieldValue)
	default:
		return false
	}
}

func compareInt(fieldValue int, searchValue string, operator event.SearchOperator) bool {
	var intValue int
	var err error

	if operator != event.SearchMatch {
		intValue, err = strconv.Atoi(searchValue)
		if err != nil {
			return false
		}
	}

	switch operator {
	case event.SearchEqual:
		return fieldValue == intValue
	case event.SearchNotEqual:
		return fieldValue != intValue
	case event.SearchGreaterThan:
		return fieldValue > intValue
	case event.SearchGreaterThanOrEqual:
		return fieldValue >= intValue
	case event.SearchLessThan:
		return fieldValue < intValue
	case event.SearchLessThanOrEqual:
		return fieldValue <= intValue
	case event.SearchMatch:
		expr := regexp.MustCompilePOSIX(searchValue)
		return expr.MatchString(strconv.Itoa(fieldValue))
	default:
		return false
	}
}

func compareTime(fieldValue time.Time, searchValue string, operator event.SearchOperator) bool {
	var timeValue time.Time
	var err error

	if operator != event.SearchMatch {
		timeValue, err = time.Parse(time.RFC3339, searchValue)
		if err != nil {
			return false
		}
	}

	switch operator {
	case event.SearchEqual:
		return fieldValue.Equal(timeValue)
	case event.SearchNotEqual:
		return !fieldValue.Equal(timeValue)
	case event.SearchGreaterThan:
		return fieldValue.After(timeValue)
	case event.SearchGreaterThanOrEqual:
		return fieldValue.After(timeValue) || fieldValue.Equal(timeValue)
	case event.SearchLessThan:
		return fieldValue.Before(timeValue)
	case event.SearchLessThanOrEqual:
		return fieldValue.Before(timeValue) || fieldValue.Equal(timeValue)
	case event.SearchMatch:
		expr := regexp.MustCompilePOSIX(searchValue)
		return expr.MatchString(fieldValue.Format(time.RFC3339))
	default:
		return false
	}
}

func sortEvents(events []event.PersistenceEvent, sortFields []event.SortField) {
	sort.SliceStable(events, func(i, j int) bool {
		for _, sortField := range sortFields {
			cmp := compareSortField(events[i], events[j], sortField)
			if cmp != 0 {
				if sortField.IsDesc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

func compareSortField(a, b event.PersistenceEvent, sortField event.SortField) int {
	switch sortField.Name {

	case event.SortAggregateID:
		return strings.Compare(a.AggregateID, b.AggregateID)
	case event.SortAggregateType:
		return strings.Compare(a.AggregateType, b.AggregateType)
	case event.SortAggregateVersion:
		return compareIntSort(a.Version, b.Version)
	case event.SortAggregateEventType:
		return strings.Compare(a.Type, b.Type)
	case event.SortAggregateClass:
		return strings.Compare(string(a.Class), string(b.Class))
	case event.SortTransactionTime:
		return compareTimeSort(a.TransactionTime, b.TransactionTime)
	case event.SortValidTime:
		return compareTimeSort(a.ValidTime, b.ValidTime)
	default:
		return 0
	}
}

func compareIntSort(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareTimeSort(a, b time.Time) int {
	if a.Before(b) {
		return -1
	}
	if a.After(b) {
		return 1
	}
	return 0
}

func paginateEvents(events []event.PersistenceEvent, page event.PageDTO) ([]event.PersistenceEvent, event.PageDTO, event.PageDTO) {
	// empty cursor -> return all events
	if len(page.Values) == 0 && page.PageSize == 0 {
		return events, page, page
	}

	var startIndex int
	// initial cursor
	if len(page.Values) == 0 && page.PageSize != 0 {
		startIndex = 0
	} else {
		for i, evt := range events {
			if findCurrentPage(evt, page.Values) {
				startIndex = i
				break
			}
		}
	}

	// Returns the events that come after the startIndex, limited to PageSize
	var endIndex int
	if page.IsBackward {
		// reveres because of backward
		tmpStart := startIndex - int(page.PageSize)
		endIndex = startIndex
		startIndex = tmpStart

	} else {
		endIndex = startIndex + int(page.PageSize) + 1
	}

	if endIndex > len(events) {
		endIndex = len(events)
	}

	paginatedEvents := events[startIndex:endIndex]

	// Initialising the Previous and Next PageDTOs
	var previousPage, nextPage event.PageDTO
	if len(paginatedEvents) > 0 {
		previousPage = page
		nextPage = page

		previousPage.Values = extractValues(paginatedEvents[0], page.SearchFields)
		previousPage.IsBackward = true

		nextPage.Values = extractValues(paginatedEvents[len(paginatedEvents)-1], page.SearchFields)
		nextPage.IsBackward = false
	}

	return paginatedEvents, previousPage, nextPage
}

func findCurrentPage(evt event.PersistenceEvent, values []interface{}) bool {
	if evt.ID != values[len(values)-1].(string) {
		return false
	}
	return true
}

func extractValues(evt event.PersistenceEvent, searchFields []event.SearchField) []interface{} {
	values := make([]interface{}, len(searchFields))
	for i, searchField := range searchFields {
		switch searchField.Name {
		case event.SearchAggregateEventID:
			values[i] = evt.ID
		case event.SearchAggregateID:
			values[i] = evt.AggregateID
		case event.SearchAggregateType:
			values[i] = evt.AggregateType
		case event.SearchAggregateVersion:
			values[i] = evt.Version
		case event.SearchAggregateEventType:
			values[i] = evt.Type
		case event.SearchAggregateClass:
			values[i] = string(evt.Class)
		case event.SearchTransactionTime:
			values[i] = evt.TransactionTime
		case event.SearchValidTime:
			values[i] = evt.ValidTime
		}
	}

	//id as last value
	values = append(values, evt.ID)
	return values
}
