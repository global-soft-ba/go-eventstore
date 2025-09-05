package extractor

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"strconv"
	"time"
)

func ExtractIntValue(ctx *gin.Context, key string, defaultValue int) int {
	value := defaultValue
	if tmp, err := strconv.ParseInt(ctx.DefaultQuery(key, strconv.FormatInt(int64(defaultValue), 10)), 10, 32); err != nil {
		logger.Warn(fmt.Errorf("could not parse %q as %s: %w", ctx.Query(key), key, err).Error())
	} else {
		value = int(tmp)
	}
	return value
}

func ExtractUintValue(ctx *gin.Context, key string, defaultValue uint) uint {
	value := defaultValue
	if tmp, err := strconv.ParseInt(ctx.DefaultQuery(key, strconv.FormatUint(uint64(defaultValue), 10)), 10, 32); err != nil {
		logger.Warn(fmt.Errorf("could not parse %q as %s: %w", ctx.Query(key), key, err).Error())
	} else {
		value = uint(tmp)
	}
	return value
}

func ExtractUintValueWithMinMax(ctx *gin.Context, key string, defaultValue, minValue, maxValue uint) uint {
	value := ExtractUintValue(ctx, key, defaultValue)
	if value < minValue {
		value = defaultValue
	} else if value > maxValue {
		value = maxValue
	}
	return value
}

func ExtractBool(ctx *gin.Context, key string, defaultValue bool) bool {
	value := defaultValue
	if tmp, err := strconv.ParseBool(ctx.DefaultQuery(key, strconv.FormatBool(defaultValue))); err != nil {
		logger.Warn(fmt.Errorf("could not parse %q as %s: %w", ctx.Query(key), key, err).Error())
	} else {
		value = tmp
	}
	return value
}

func ExtractTimeValue(ctx *gin.Context, key string, defaultTime time.Time) (time.Time, error) {
	timeString := ctx.Query(key)

	if timeString == "" {
		return defaultTime, nil
	}

	parsedTime, err := time.Parse(time.RFC3339, timeString)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse %q as %s: %w", timeString, key, err)
	}

	return parsedTime, nil
}
