package rest

import (
	"context"
	eventService "example/core/port/service/event"
	itemService "example/core/port/service/item"
	projectionService "example/core/port/service/projection"
	seederService "example/core/port/service/seeder"
	"example/docs"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"golang.org/x/sync/errgroup"
	"net/http"
	"time"
)

type Options struct {
	Port            int
	ShutdownTimeout time.Duration
}

type Controller struct {
	options Options
	router  *gin.Engine

	item       itemService.Port
	projection projectionService.Port
	seeder     seederService.Port
	event      eventService.Port
}

func NewController(ctx context.Context, options Options, item itemService.Port, projection projectionService.Port, seeder seederService.Port, event eventService.Port) *Controller {
	ctx, endSpan := metrics.StartSpan(ctx, "init-rest-controller", map[string]interface{}{"port": options.Port})
	defer endSpan()

	docs.SwaggerInfo.Title = "Example EventStore Application"
	docs.SwaggerInfo.Description = "This is example application of Go EventStore library."
	docs.SwaggerInfo.Version = "1.0"

	gin.SetMode(gin.DebugMode)

	c := &Controller{
		options:    options,
		router:     gin.Default(),
		item:       item,
		projection: projection,
		seeder:     seeder,
		event:      event,
	}
	c.setupRoutes()
	return c
}

func (c *Controller) setupRoutes() {

	c.router.GET("swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	api := c.router.Group("api/:tenantID")
	{
		items := api.Group("items")
		{
			items.POST("", c.createItem)
			items.GET("", c.findItems)
			items.GET(":itemID", c.findItemByID)
			items.PATCH(":itemID/rename", c.renameItem)
			items.DELETE(":itemID", c.deleteItem)
		}
		projections := api.Group("projections")
		{
			projections.GET("", c.getProjectionStates)
			projections.DELETE(":projectionID", c.deleteProjection)
			projections.POST("rebuild", c.rebuildProjections)
			projections.POST("start", c.startProjections)
			projections.POST("stop", c.stopProjections)
		}
		events := api.Group("events")
		{
			events.GET("", c.getAggregateEvents)
			events.DELETE(":aggregateType/:aggregateID/:eventID", c.deleteScheduledEvent)
		}
		seeders := api.Group("seeders")
		{
			seeders.POST("items", c.seedItems)
		}
	}
}

func (c *Controller) Run(ctx context.Context) error {
	server := http.Server{
		Addr: fmt.Sprintf(":%d", c.options.Port),
	}

	_ = c.router.Run(server.Addr)

	group, ctx := errgroup.WithContext(ctx)

	group.Go(
		func() error {
			return server.ListenAndServe()
		})

	group.Go(
		func() error {
			<-ctx.Done()
			ctx, cancel := context.WithTimeout(ctx, c.options.ShutdownTimeout)
			defer cancel()
			return server.Shutdown(ctx)
		})

	return group.Wait()

}
