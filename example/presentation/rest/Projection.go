package rest

import (
	"example/presentation/response"
	"github.com/gin-gonic/gin"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"net/http"
)

// getProjectionStates godoc
//
//	@Summary		Get projection states
//	@Description	Get the states of projections for a specific tenant
//	@Tags			Projection
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string		true	"Tenant ID"
//	@Param			projections	query		[]string	true	"Projection IDs"	Enums(item)
//	@Success		200			{array}		projection.StateResponseDTO
//	@Failure		500			{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/projections [get]
func (c *Controller) getProjectionStates(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	projections := ctx.QueryArray("projections")

	logger.Info("HTTP request received: Getting projection states for tenantID: %q, projections: %v", tenantID, projections)

	states, err := c.projection.GetProjectionStates(ctx, tenantID, projections...)
	if err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Found projection states for tenantID: %q, projections: %v", tenantID, projections)
	ctx.JSON(http.StatusOK, states)
}

// deleteProjection godoc
//
//	@Summary		Delete a projection
//	@Description	Delete a projection by its ID for a specific tenant
//	@Tags			Projection
//	@Accept			json
//	@Produce		json
//	@Param			tenantID		path		string	true	"Tenant ID"
//	@Param			projectionID	path		string	true	"Projection ID"	example("item")
//	@Success		201				{object}	nil
//	@Failure		500				{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/projections/{projectionID} [delete]
func (c *Controller) deleteProjection(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	projectionID := ctx.Param("projectionID")

	logger.Info("HTTP request received: Deleting projection for tenantID: %q, projectionID: %q", tenantID, projectionID)

	if err := c.projection.DeleteProjection(ctx, tenantID, projectionID); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Projection deleted successfully for tenantID: %q, projectionID: %q", tenantID, projectionID)

	ctx.JSON(http.StatusCreated, nil)
}

// rebuildProjections godoc
//
//	@Summary		Rebuild projections
//	@Description	Rebuild specified projections for a tenant
//	@Tags			Projection
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string		true	"Tenant ID"
//	@Param			projections	query		[]string	true	"Projections"	Enums(item)
//	@Success		200			{object}	nil
//	@Failure		500			{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/projections/rebuild [post]
func (c *Controller) rebuildProjections(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	projections := ctx.QueryArray("projections")

	logger.Info("HTTP request received: Rebuilding projections for tenantID: %q, projections: %v", tenantID, projections)

	if err := c.projection.RebuildProjections(ctx, tenantID, projections...); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Projections rebuilt successfully for tenantID: %q, projections: %v", tenantID, projections)
	ctx.JSON(http.StatusOK, nil)
}

// startProjections godoc
//
//	@Summary		Start projections
//	@Description	Start specified projections for a tenant
//	@Tags			Projection
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string		true	"Tenant ID"
//	@Param			projections	query		[]string	true	"Projections"	Enums(item)
//	@Success		200			{object}	nil
//	@Failure		500			{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/projections/start [post]
func (c *Controller) startProjections(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	projections := ctx.QueryArray("projections")

	logger.Info("HTTP request received: Starting projections for tenantID: %q, projections: %v", tenantID, projections)

	if err := c.projection.StartProjections(ctx, tenantID, projections...); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Projections started successfully for tenantID: %q, projections: %v", tenantID, projections)
	ctx.JSON(http.StatusOK, nil)
}

// stopProjections godoc
//
//	@Summary		Stop projections
//	@Description	Stop specified projections for a tenant
//	@Tags			Projection
//	@Accept			json
//	@Produce		json
//	@Param			tenantID	path		string		true	"Tenant ID"
//	@Param			projections	query		[]string	true	"Projections"	Enums(item)
//	@Success		200			{object}	nil
//	@Failure		500			{object}	response.ErrorResponse
//	@Router			/api/{tenantID}/projections/stop [post]
func (c *Controller) stopProjections(ctx *gin.Context) {
	tenantID := ctx.Param("tenantID")
	projections := ctx.QueryArray("projections")

	logger.Info("HTTP request received: Stopping projections for tenantID: %q, projections: %v", tenantID, projections)

	if err := c.projection.StopProjections(ctx, tenantID, projections...); err != nil {
		logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, response.NewErrorResponse(err))
		return
	}

	logger.Info("HTTP request completed: Projections stopped successfully for tenantID: %q, projections: %v", tenantID, projections)
	ctx.JSON(http.StatusOK, nil)
}
