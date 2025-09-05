package aggregate

import (
	"encoding/json"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"time"
)

func (s *Stream) DeleteEvent(evt event.PersistenceEvent, userID string) (event.PersistenceEvent, error) {
	switch evt.Class {
	case event.CreateStreamEvent:
		return evt, fmt.Errorf("cannot delete create stream event")
	case event.SnapShot, event.HistoricalSnapShot:
		return evt, fmt.Errorf("cannot delete snapshot event")
	default:
	}

	switch s.Options().DeleteStrategy {
	case event.HardDelete:
		evt.Class = event.DeletePatch
		return evt, nil
	case event.SoftDelete:
		evt.Class = event.DeletePatch
		return s.markAsDeleted(evt, userID)
	default:
		return evt, fmt.Errorf("unknown delete strategy %v", s.Options().DeleteStrategy)
	}
}

func (s *Stream) markAsDeleted(evt event.PersistenceEvent, userID string) (event.PersistenceEvent, error) {
	var existingDataFields map[string]interface{}
	if err := json.Unmarshal(evt.Data, &existingDataFields); err != nil {
		return evt, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	existingDataFields["Deleted"] = event.Deleted{
		DeletedAt: time.Now(),
		DeletedBy: userID,
	}

	updatedRaw, err := json.Marshal(existingDataFields)
	if err != nil {
		return evt, err
	}

	evt.Data = updatedRaw
	return evt, nil
}
