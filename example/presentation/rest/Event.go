package rest

import (
	"example/core/port/service/event"
	"example/presentation/extractor"
	"example/presentation/paginator"
	"example/presentation/response"
	"github.com/gin-gonic/gin"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"net/http"
	"time"
)

type EventStreamResponse struct {
	Patches   []event.DTO      `json:"patches"`
	Previous  response.PageDTO `json:"previous"`
	Next      response.PageDTO `json:"next"`
	ValidTime time.Time        `json:"validTime,omitempty" time_format:"RFC3339" example:"2024-01-03T00:00:00Z"`
}

// getAggregateEvents godoc
//
//	@Summary		Get aggregate events
//	@Description	Get all aggregate events (historical events and future patches) with optional search, sorting and pagination. The search is case-insensitive and has filter support via "@ColumnName:X". If no column is specified, all possible search fields are searched. Sorting for multiple columns via "+ColumnName,-ColumnName2" with +/- prefix to specify ascending/descending sort direction. The size parameter specifies the maximum number of entries that will be returned. With the values and direction parameters the start value of the page can be defined to achieve pagination.
//	@Tags			Event
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string					true	"Tenant ID"
//	@Param			time		query		string					false	"Time (RFC3339)"																																																																											example("2024-01-03T00:00:00Z")
//	@Param			search		query		string					false	"Search Term - supported columns are: EventID/SearchAggregateEventID, AggregateType/SearchAggregateType, AggregateID/SearchAggregateID, Version/SearchAggregateVersion, EventType/SearchAggregateEventType, Class/SearchAggregateClass, ValidTime/SearchValidTime, TransactionTime/SearchTransactionTime, Data/SearchData"	default(@AggregateType:Item)
//	@Param			sort		query		string					false	"Sort Term - supported columns are: AggregateType/SortAggregateType, AggregateID/SortAggregateID, Version/SortAggregateVersion, EventType/SortAggregateEventType, Class/SortAggregateClass, ValidTime/SortValidTime, TransactionTime/SortTransactionTime"																	default(-ValidTime)
//	@Param			size		query		uint					false	"Page Size - maximum 1000"																																																																									default(1000)
//	@Param			values		query		[]string				false	"Values of first row on page (only for pagination)"
//	@Param			direction	query		bool					false	"IsBackward (only for pagination)"	default(false)
//	@Success		200			{object}	EventStreamResponse		"Success: Found Events"
//	@Failure		400			{object}	response.ErrorResponse	"Error: Bad Request"
//	@Failure		500			{object}	response.ErrorResponse	"Error: Internal Server Error"
//	@Router			/api/{tenantID}/events [get]
func (c *Controller) getAggregateEvents(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")

	logger.Info("HTTP request received: Getting aggregate events for tenantID: %q", tenantID)

	page, err := paginator.ParsePaginationAndSearch(ctx, event.SortMap, event.SearchMap, event.SortDefaultField, event.MaxPageSizeEvents)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusBadRequest, response.NewErrorResponse(err))
		return
	}
	pageDTO := event.FromDTO(page)

	validTime, err := extractor.ExtractTimeValue(ctx, "time", time.Now())
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusBadRequest, response.NewErrorResponse(err))
		return
	}

	patches, pages, err := c.event.GetAggregateEvents(ctx, tenantID, pageDTO)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Found %d events for tenantID: %q, aggregateType: %q, aggregateID: %q", len(patches), tenantID)
	ctx.JSON(http.StatusOK, EventStreamResponse{
		Patches: patches,
		Previous: response.PageDTO{
			Search:   page.SearchFields.ToQueryParam(),
			Sort:     page.SortFields.ToQueryParam(),
			Size:     page.PageSize,
			Values:   pages.Previous.Values,
			Backward: true,
		},
		Next: response.PageDTO{
			Search:   page.SearchFields.ToQueryParam(),
			Sort:     page.SortFields.ToQueryParam(),
			Size:     page.PageSize,
			Values:   pages.Next.Values,
			Backward: false,
		},
		ValidTime: validTime,
	})
}

// deleteScheduledEvent godoc
//
//	@Summary		Delete scheduled event
//	@Description	Delete scheduled event of an aggregate
//	@Tags			Event
//	@Accept			json
//	@Produce		json
//	@Param			tenantID		path		string	true	"Tenant ID"
//	@Param			aggregateType	path		string	true	"Aggregate Type"	example("Item")
//	@Param			aggregateID		path		string	true	"Aggregate ID"
//	@Param			eventID			path		string	true	"Event ID"
//	@Success		200				{object}	nil
//	@Failure		500				{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/events/{aggregateType}/{aggregateID}/{eventID} [delete]
func (c *Controller) deleteScheduledEvent(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	aggregateType := ctx.Param("aggregateType")
	aggregateID := ctx.Param("aggregateID")
	eventID := ctx.Param("eventID")

	logger.Info("HTTP request received: Deleting scheduled event for tenantID: %q, aggregateType: %q, aggregateID: %q, eventID: %q", tenantID, aggregateType, aggregateID, eventID)

	err := c.event.DeleteScheduledEvent(ctx, tenantID, aggregateID, aggregateType, eventID)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Scheduled event deleted for tenantID: %q, aggregateType: %q, aggregateID: %q, eventID: %q", tenantID, aggregateType, aggregateID, eventID)
	ctx.JSON(http.StatusOK, nil)
}
