package event

import (
	"context"
	"example/presentation/paginator"
	"github.com/global-soft-ba/go-eventstore"
	"strings"
	"time"
)

const (
	MaxPageSizeEvents uint = 1000

	SortDefaultField = event.SortValidTime
)

var (
	SortMap = map[string]string{
		"AggregateType":              event.SortAggregateType,
		event.SortAggregateType:      event.SortAggregateType,
		"AggregateID":                event.SortAggregateID,
		event.SortAggregateID:        event.SortAggregateID,
		"Version":                    event.SortAggregateVersion,
		event.SortAggregateVersion:   event.SortAggregateVersion,
		"EventType":                  event.SortAggregateEventType,
		event.SortAggregateEventType: event.SortAggregateEventType,
		"Class":                      event.SortAggregateClass,
		event.SortAggregateClass:     event.SortAggregateClass,
		"ValidTime":                  event.SortValidTime,
		event.SortValidTime:          event.SortValidTime,
		"TransactionTime":            event.SortTransactionTime,
		event.SortTransactionTime:    event.SortTransactionTime,
	}

	SearchMap = map[string]string{
		"EventID":                      event.SearchAggregateEventID,
		event.SearchAggregateEventID:   event.SearchAggregateEventID,
		"AggregateType":                event.SearchAggregateType,
		event.SearchAggregateType:      event.SearchAggregateType,
		"AggregateID":                  event.SearchAggregateID,
		event.SearchAggregateID:        event.SearchAggregateID,
		"Version":                      event.SearchAggregateVersion,
		event.SearchAggregateVersion:   event.SearchAggregateVersion,
		"EventType":                    event.SearchAggregateEventType,
		event.SearchAggregateEventType: event.SearchAggregateEventType,
		"Class":                        event.SearchAggregateClass,
		event.SearchAggregateClass:     event.SearchAggregateClass,
		"ValidTime":                    event.SearchValidTime,
		event.SearchValidTime:          event.SearchValidTime,
		"TransactionTime":              event.SearchTransactionTime,
		event.SearchTransactionTime:    event.SearchTransactionTime,
		"Data":                         event.SearchData,
		event.SearchData:               event.SearchData,
	}
)

type Port interface {
	GetAggregateEvents(ctx context.Context, tenantID string, pageDTO event.PageDTO) ([]DTO, event.PagesDTO, error)
	DeleteScheduledEvent(ctx context.Context, tenantID, aggregateID, aggregateType, eventID string) error
}

type DTO struct {
	ValidFrom     time.Time `json:"validFrom"`
	EditedBy      string    `json:"editedBy"`
	Field         string    `json:"field"`
	ValueChange   []byte    `json:"valueChange"`
	ScheduledAt   time.Time `json:"scheduledAt"`
	AggregateID   string    `json:"aggregateID"`
	AggregateType string    `json:"aggregateType"`
	EventID       string    `json:"eventID"`
}

func FromDTO(page paginator.PageDTO) event.PageDTO {
	pageDTO := event.PageDTO{
		PageSize:   page.PageSize,
		Values:     page.Values,
		IsBackward: page.IsBackward,
	}
	for _, field := range page.SortFields {
		pageDTO.SortFields = append(pageDTO.SortFields, event.SortField{
			Name:   field.FieldID,
			IsDesc: field.Desc,
		})
	}
	for _, field := range page.SearchFields {
		//Legacy support for passing SearchOperator via separated field
		if strings.HasPrefix(strings.ToLower(field.Name), strings.ToLower("SearchOperator")) && field.FieldID == paginator.SearchAny {
			searchField := pageDTO.SearchFields[len(pageDTO.SearchFields)-1]
			searchField.Operator = event.SearchOperator(field.Operator)
			pageDTO.SearchFields[len(pageDTO.SearchFields)-1] = searchField
			continue
		}
		pageDTO.SearchFields = append(pageDTO.SearchFields, event.SearchField{
			Name:     field.FieldID,
			Value:    field.Value,
			Operator: event.SearchOperator(field.Operator),
		})
	}
	return pageDTO
}
