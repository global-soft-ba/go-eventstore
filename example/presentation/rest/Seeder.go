package rest

import (
	"example/core/port/service/seeder"
	"example/presentation/response"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"net/http"
	"strconv"
)

// seedItems godoc
//
//	@Summary		Seed items
//	@Description	Seed a specific number of items for a tenant
//	@Tags			Seeder
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string	true	"Tenant ID"
//	@Param			count		query		int		false	"Number of items to seed"	default(1000)
//	@Success		201			{object}	nil
//	@Failure		500			{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/seeders/items [post]
func (c *Controller) seedItems(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")

	logger.Info("HTTP request received: Seeding items for tenantID: %q", tenantID)

	countQueryParam := ctx.DefaultQuery("count", seeder.DefaultSeedItemsCount)
	count, convErr := strconv.Atoi(countQueryParam)
	if convErr != nil {
		err := fmt.Errorf("invalid count parameter: %w", convErr)
		logger.Error(err)
		ctx.JSON(http.StatusBadRequest, response.NewErrorResponse(err))
		return
	}

	err := c.seeder.SeedItems(ctx, tenantID, count)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Successfully seeded %d items for tenantID: %q", count, tenantID)
	ctx.JSON(http.StatusCreated, nil)
}
