package timespan

import (
	"reflect"
	"testing"
	"time"
)

func TestSpan_Excepts(t *testing.T) {

	type fields struct {
		span Span
	}
	type args struct {
		input Spans
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Spans
	}{
		{
			name: "happy path single interval",
			fields: fields{span: newForTest(
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 0, 0, 0, 21, time.UTC))},
			args: args{
				func() Spans {
					i1, _ := New(
						time.Date(2021, 1, 1, 0, 0, 0, 5, time.UTC),
						time.Date(2021, 1, 1, 0, 0, 0, 11, time.UTC))
					return NewSpans(i1)
				}(),
			},
			want: NewSpans(
				newForTest(
					time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2021, 1, 1, 0, 0, 0, 5, time.UTC),
				),
				newForTest(
					time.Date(2021, 1, 1, 0, 0, 0, 11, time.UTC),
					time.Date(2021, 1, 1, 0, 0, 0, 21, time.UTC),
				),
			),
		},
		{
			name: "happy path multiple interval",
			fields: fields{span: newForTest(
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 0, 0, 0, 21, time.UTC))},
			args: args{
				func() Spans {
					i1, _ := New(
						time.Date(2021, 1, 1, 0, 0, 0, 5, time.UTC),
						time.Date(2021, 1, 1, 0, 0, 0, 11, time.UTC))

					i2, _ := New(
						time.Date(2021, 1, 1, 0, 0, 0, 4, time.UTC),
						time.Date(2021, 1, 1, 0, 0, 0, 10, time.UTC))

					i3, _ := New(
						time.Date(2021, 1, 1, 0, 0, 0, 18, time.UTC),
						time.Date(2021, 1, 1, 0, 0, 0, 19, time.UTC))
					return NewSpans(i1, i2, i3)
				}(),
			},
			want: NewSpans(
				newForTest(
					time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2021, 1, 1, 0, 0, 0, 4, time.UTC),
				),
				newForTest(
					time.Date(2021, 1, 1, 0, 0, 0, 11, time.UTC),
					time.Date(2021, 1, 1, 0, 0, 0, 18, time.UTC),
				),
				newForTest(
					time.Date(2021, 1, 1, 0, 0, 0, 19, time.UTC),
					time.Date(2021, 1, 1, 0, 0, 0, 21, time.UTC),
				),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got := tt.fields.span.Excepts(tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Excepts()\n got = %v \nwant = %v", got.String(), tt.want.String())
			}
		})
	}
}
