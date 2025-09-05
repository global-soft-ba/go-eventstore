package mapper

import (
	"encoding/json"
	service "example/core/port/service/event"
	"github.com/global-soft-ba/go-eventstore"
	"strings"
)

func FromEventStoreToServiceDTOs(dtos []event.PersistenceEvent) []service.DTO {
	var serviceDTOs []service.DTO
	for _, dto := range dtos {
		serviceDTOs = append(serviceDTOs, FromEventStoreToServiceDTO(dto))
	}
	return serviceDTOs
}

func FromEventStoreToServiceDTO(pEvt event.PersistenceEvent) service.DTO {
	var rootEvt event.Event

	if err := json.Unmarshal(pEvt.Data, &rootEvt); err != nil {
		return service.DTO{}
	}

	field, _ := strings.CutPrefix(pEvt.Type, "example/core/domain/model/")
	return service.DTO{
		ValidFrom:     pEvt.ValidTime,
		EditedBy:      rootEvt.UserID,
		Field:         field,
		ValueChange:   pEvt.Data,
		ScheduledAt:   pEvt.TransactionTime,
		AggregateID:   pEvt.AggregateID,
		AggregateType: pEvt.AggregateType,
		EventID:       pEvt.ID,
	}
}
