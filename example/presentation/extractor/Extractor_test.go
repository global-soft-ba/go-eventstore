//go:build unit

package extractor

import (
	"example/presentation/test"
	"github.com/gin-gonic/gin"
	"reflect"
	"testing"
	"time"
)

func TestExtractIntValue(t *testing.T) {
	type args struct {
		ctx          *gin.Context
		key          string
		defaultValue int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "No query param, default value",
			args: args{
				ctx:          test.GetTestContext(""),
				key:          "limit",
				defaultValue: 10,
			},
			want: 10,
		},
		{
			name: "query param parseable",
			args: args{
				ctx:          test.GetTestContext("limit=100"),
				key:          "limit",
				defaultValue: 10,
			},
			want: 100,
		},
		{
			name: "query param not parseable, default value",
			args: args{
				ctx:          test.GetTestContext("limit=not_a_number"),
				key:          "limit",
				defaultValue: 10,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractIntValue(tt.args.ctx, tt.args.key, tt.args.defaultValue); got != tt.want {
				t.Errorf("ExtractIntValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractUintValueWithMinMax(t *testing.T) {
	type args struct {
		ctx          *gin.Context
		key          string
		defaultValue uint
		minValue     uint
		maxValue     uint
	}
	tests := []struct {
		name string
		args args
		want uint
	}{
		{
			name: "No query param, default value",
			args: args{
				ctx:          test.GetTestContext(""),
				key:          "size",
				defaultValue: 10,
				minValue:     1,
				maxValue:     100,
			},
			want: 10,
		},
		{
			name: "query param in bounds",
			args: args{
				ctx:          test.GetTestContext("size=20"),
				key:          "size",
				defaultValue: 10,
				minValue:     1,
				maxValue:     100,
			},
			want: 20,
		},
		{
			name: "query param too high, use max value",
			args: args{
				ctx:          test.GetTestContext("size=200"),
				key:          "size",
				defaultValue: 10,
				minValue:     1,
				maxValue:     100,
			},
			want: 100,
		},
		{
			name: "query param too low, use default value",
			args: args{
				ctx:          test.GetTestContext("size=0"),
				key:          "size",
				defaultValue: 10,
				minValue:     1,
				maxValue:     100,
			},
			want: 10,
		},
		{
			name: "query param not parsable, default value",
			args: args{
				ctx:          test.GetTestContext("size=not_a_number"),
				key:          "size",
				defaultValue: 10,
				minValue:     1,
				maxValue:     100,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractUintValueWithMinMax(tt.args.ctx, tt.args.key, tt.args.defaultValue, tt.args.minValue, tt.args.maxValue); got != tt.want {
				t.Errorf("ExtractUintValueWithMinMax() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractBool(t *testing.T) {
	type args struct {
		ctx          *gin.Context
		key          string
		defaultValue bool
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "no query param, default value",
			args: args{
				ctx:          test.GetTestContext(""),
				key:          "active",
				defaultValue: true,
			},
			want: true,
		},
		{
			name: "query param beats default value",
			args: args{
				ctx:          test.GetTestContext("active=true"),
				key:          "active",
				defaultValue: false,
			},
			want: true,
		},
		{
			name: "query param not parsable, default value true",
			args: args{
				ctx:          test.GetTestContext("active=not_a_bool"),
				key:          "active",
				defaultValue: true,
			},
			want: true,
		},
		{
			name: "query param not parsable, default value false",
			args: args{
				ctx:          test.GetTestContext("active=not_a_bool"),
				key:          "active",
				defaultValue: false,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractBool(tt.args.ctx, tt.args.key, tt.args.defaultValue); got != tt.want {
				t.Errorf("ExtractBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractTimeValue(t *testing.T) {
	defaultTime := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	type args struct {
		ctx         *gin.Context
		key         string
		defaultTime time.Time
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr bool
	}{
		{
			name: "no query param, default value",
			args: args{
				ctx:         test.GetTestContext(""),
				key:         "valid_time",
				defaultTime: defaultTime,
			},
			want:    defaultTime,
			wantErr: false,
		},
		{
			name: "query param contains valid date",
			args: args{
				ctx:         test.GetTestContext("valid_time=2006-01-02T15:04:05Z"),
				key:         "valid_time",
				defaultTime: defaultTime,
			},
			want:    time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC),
			wantErr: false,
		},
		{
			name: "query param contains invalid date",
			args: args{
				ctx:         test.GetTestContext("valid_time=not_a_date"),
				key:         "valid_time",
				defaultTime: defaultTime,
			},
			want:    time.Time{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractTimeValue(tt.args.ctx, tt.args.key, tt.args.defaultTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractTimeValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractTimeValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}
