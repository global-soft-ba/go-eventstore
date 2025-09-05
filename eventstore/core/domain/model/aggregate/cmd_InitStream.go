package aggregate

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"time"
)

func (s *Stream) InitStream(evt event.PersistenceEvent, created time.Time) error {
	if s.currentVersion > 0 {
		return fmt.Errorf("stream already created")
	}

	if evt.Class != event.CreateStreamEvent {
		return fmt.Errorf("wrong event class for create")
	}
	if evt.Version != initVersion {
		return fmt.Errorf("wrong version number for create statement of aggregate %q with type %q of teanant %q", evt.AggregateID, evt.AggregateType, evt.TenantID)
	}

	if evt.FromMigration {
		s.createTime = evt.ValidTime
		s.lastTransactionTime = evt.TransactionTime
		s.latestValidTime = evt.ValidTime
	} else if !evt.ValidTime.IsZero() {
		s.createTime = evt.ValidTime
		s.lastTransactionTime = created
		s.latestValidTime = evt.ValidTime
	} else {
		s.createTime = created
		s.lastTransactionTime = created
		s.latestValidTime = created
	}

	return nil
}
