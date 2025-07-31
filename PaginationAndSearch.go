package event

const (
	SortAggregateType      = "SortAggregateType"
	SortAggregateID        = "SortAggregateID"
	SortAggregateVersion   = "SortAggregateVersion"
	SortAggregateEventType = "SortAggregateEventType"
	SortAggregateClass     = "SortAggregateClass"
	SortValidTime          = "SortValidTime"
	SortTransactionTime    = "SortTransactionTime"
)

const (
	SearchAggregateEventID   = "SearchAggregateEventID"
	SearchAggregateType      = "SearchAggregateType"
	SearchAggregateID        = "SearchAggregateID"
	SearchAggregateVersion   = "SearchAggregateVersion"
	SearchAggregateEventType = "SearchAggregateEventType"
	SearchAggregateClass     = "SearchAggregateClass"
	SearchValidTime          = "SearchValidTime"       //as RFC3339
	SearchTransactionTime    = "SearchTransactionTime" //As RFC3339
	SearchData               = "SearchData"
)

type SortField struct {
	Name   string `json:"name"`
	IsDesc bool   `json:"desc"`
}

type SearchField struct {
	Name     string         `json:"name"`
	Value    string         `json:"value"`
	Operator SearchOperator `json:"comparison"`
}

type SearchOperator string

const (
	SearchGreaterThan        SearchOperator = ">"
	SearchGreaterThanOrEqual SearchOperator = ">="
	SearchLessThan           SearchOperator = "<"
	SearchLessThanOrEqual    SearchOperator = "<="
	SearchEqual              SearchOperator = "="
	SearchNotEqual           SearchOperator = "!="
	SearchMatch              SearchOperator = "~*"
)

type PagesDTO struct {
	Previous PageDTO `json:"previous"`
	Next     PageDTO `json:"next"`
}

type PageDTO struct {
	PageSize     uint          `json:"pageSize"`
	SortFields   []SortField   `json:"sortFields"`
	SearchFields []SearchField `json:"searchFields"`
	Values       []interface{} `json:"values"`
	IsBackward   bool          `json:"isBackward"`
}
