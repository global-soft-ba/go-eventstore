package rest

import (
	itemRead "example/core/port/readmodel/item"
	itemService "example/core/port/service/item"
	"example/presentation/extractor"
	"example/presentation/paginator"
	"example/presentation/response"
	"github.com/gin-gonic/gin"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"net/http"
	"time"
)

// createItem godoc
//
//	@Summary		Create a new item
//	@Description	Create a new item for a specific tenant
//	@Tags			Item
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string					true	"Tenant ID"
//	@Param			item		body		itemService.CreateDTO	true	"Item Data"
//	@Success		201			{object}	nil
//	@Failure		400			{object}	response.ErrorResponse
//	@Failure		500			{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/items [post]
func (c *Controller) createItem(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")

	logger.Info("HTTP request received: Creating item for tenantID: %q", tenantID)

	var createDTO itemService.CreateDTO
	if err := ctx.ShouldBindJSON(&createDTO); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusBadRequest, response.NewErrorResponse(err))
		return
	}
	dto := itemService.DTO{
		TenantID: tenantID,
		ItemID:   createDTO.ItemID,
		ItemName: createDTO.ItemName,
	}
	if err := c.item.CreateItem(ctx, dto); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Item created successfully for tenantID: %q", tenantID)
	ctx.Status(http.StatusCreated)
}

type ItemOverviewResponse struct {
	Items    []itemService.DTO `json:"items"`
	Previous response.PageDTO  `json:"previous"`
	Next     response.PageDTO  `json:"next"`
}

// findItems godoc
//
//	@Summary		Get Items
//	@Description	Get all Items with optional search, sorting, and pagination. The search is case-insensitive and has filter support via "@ColumnName:X". If no column is specified, all possible search fields are searched. Sorting for multiple columns via "+ColumnName,-ColumnName2" with +/- prefix to specify ascending/descending sort direction. The size parameter specifies the maximum number of entries that will be returned. With the values and direction parameters the start value of the page can be defined to achieve pagination.
//	@Tags			Item
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string					true	"Tenant ID"
//	@Param			search		query		string					false	"Search Term - supported columns are: ItemID, ItemName"		default(@ItemName:Item 1)
//	@Param			sort		query		string					false	"Sort Term - supported columns are: ID, ItemID, ItemName"	default(-ItemName)
//	@Param			size		query		uint					false	"Page Size - maximum 1000"									default(1000)
//	@Param			values		query		[]string				false	"Values of first row on page (only for pagination)"
//	@Param			direction	query		bool					false	"IsBackward (only for pagination)"	default(false)
//	@Success		200			{object}	ItemOverviewResponse	"Success: Found Items"
//	@Failure		400			{object}	response.ErrorResponse	"Error: Bad Request"
//	@Failure		500			{object}	response.ErrorResponse	"Error: Internal Server Error"
//	@Router			/api/{tenantID}/items [get]
func (c *Controller) findItems(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")

	logger.Info("HTTP request received: Requesting items overview for tenantID: %q", tenantID)

	page, err := paginator.ParsePaginationAndSearch(ctx, itemRead.SortMap, itemRead.SearchMap, itemRead.SortDefaultField, itemRead.MaxPageSize)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusBadRequest, response.NewErrorResponse(err))
		return
	}
	pageDTO := itemRead.FromDTO(page)
	items, pages, err := c.item.FindItems(ctx, tenantID, pageDTO)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Item overview successfully retrieved for tenantID: %q", tenantID)
	ctx.JSON(http.StatusOK, ItemOverviewResponse{
		Items: items,
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
	})
}

// findItemByID godoc
//
//	@Summary		Get item by ID
//	@Description	Get a specific item by its ID for a specific tenant
//	@Tags			Item
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string					true	"Tenant ID"
//	@Param			itemID		path		string					true	"Item ID"
//	@Success		200			{object}	itemService.DTO			"Success: Found Item"
//	@Failure		500			{object}	response.ErrorResponse	"Error: Internal Server Error"
//	@Router			/api/{tenantID}/items/{itemID} [get]
func (c *Controller) findItemByID(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	itemID := ctx.Param("itemID")

	logger.Info("HTTP request received: Requesting item by tenantID: %q, itemID: %q", tenantID, itemID)

	item, err := c.item.FindItemByID(ctx, tenantID, itemID)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Item retrieved successfully for tenantID: %q, itemID: %q", tenantID, itemID)
	ctx.JSON(http.StatusOK, item)
}

// renameItem godoc
//
//	@Summary		Rename an item
//	@Description	Rename a specific item by its ID for a specific tenant
//	@Tags			Item
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string					true	"Tenant ID"
//	@Param			itemID		path		string					true	"Item ID"
//	@Param			validTime	query		string					false	"Valid Time"	default(2000-01-01T00:00:00Z)
//	@Param			renameDTO	body		itemService.RenameDTO	true	"Rename Data"
//	@Success		204			{object}	nil
//	@Failure		400			{object}	response.ErrorResponse	"Error: Bad Request"
//	@Failure		500			{object}	response.ErrorResponse	"Error: Internal Server Error"
//	@Router			/api/{tenantID}/items/{itemID}/rename [patch]
func (c *Controller) renameItem(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	itemID := ctx.Param("itemID")

	logger.Info("HTTP request received: Requesting item rename for tenantID: %q, itemID: %q", tenantID, itemID)

	validTime, err := extractor.ExtractTimeValue(ctx, "validTime", time.Time{})
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusBadRequest, response.NewErrorResponse(err))
		return
	}

	var renameDTO itemService.RenameDTO
	if err = ctx.ShouldBindJSON(&renameDTO); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusBadRequest, response.NewErrorResponse(err))
		return
	}

	if err = c.item.RenameItem(ctx, tenantID, itemID, renameDTO.Name, validTime); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Item renamed successfully for tenantID: %q, itemID: %q", tenantID, itemID)
	ctx.Status(http.StatusNoContent)
}

// deleteItem godoc
//
//	@Summary		Delete an item
//	@Description	Delete a specific item by its ID for a specific tenant
//	@Tags			Item
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string	true	"Tenant ID"
//	@Param			itemID		path		string	true	"Item ID"
//	@Success		204			{object}	nil
//	@Failure		500			{object}	response.ErrorResponse	"Error: Internal Server Error"
//	@Router			/api/{tenantID}/items/{itemID} [delete]
func (c *Controller) deleteItem(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	itemID := ctx.Param("itemID")

	logger.Info("HTTP request received: Requesting item delete for tenantID: %q, itemID: %q", tenantID, itemID)

	if err := c.item.DeleteItem(ctx, tenantID, itemID); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Item deleted successfully for tenantID: %q, itemID: %q", tenantID, itemID)
	ctx.Status(http.StatusNoContent)
}
