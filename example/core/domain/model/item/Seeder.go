package item

import (
	"fmt"
	"github.com/google/uuid"
	"time"
)

func SeedItems(tenantID string, num int) ([]Item, error) {
	var items []Item
	for i := 0; i < num; i++ {
		itemID := uuid.NewString()
		itemName := fmt.Sprintf("Item %d", i+1)

		item, err := CreateItem(itemID, tenantID)
		if err != nil {
			return nil, err
		}
		item, err = item.Rename(itemName, time.Time{})
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}
