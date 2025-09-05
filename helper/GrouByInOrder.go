package projection

import (
	"github.com/global-soft-ba/go-eventstore"
)

// GroupByEventTypeInOrder groups events by event type and keeps
// the order of the events given in the input slice.
func GroupByEventTypeInOrder(input []event.IEvent) [][]event.IEvent {
	grouped := make([][]event.IEvent, 0)

	if len(input) == 0 {
		return grouped
	}

	currentGroup := []event.IEvent{input[0]}

	for i := 1; i < len(input); i++ {
		if event.EventType(input[i]) == event.EventType(currentGroup[0]) {
			currentGroup = append(currentGroup, input[i])
		} else {
			grouped = append(grouped, currentGroup)
			currentGroup = []event.IEvent{input[i]}
		}
	}
	grouped = append(grouped, currentGroup)
	return grouped
}
