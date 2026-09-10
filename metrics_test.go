package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	ydbmetrics "github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestCounterVecDifferentLabelNamesNotCachedTogether(t *testing.T) {
	cfg, ok := metricsConfigFromOpts(noop.NewMeterProvider().Meter("test")).(*metricsConfig)
	require.True(t, ok)

	vecA := cfg.CounterVec("requests", "status")
	vecB := cfg.CounterVec("requests", "method")

	require.NotSame(t, vecA, vecB)
}

func TestGaugeVecWithDifferentExtraLabelsNotCachedTogether(t *testing.T) {
	cfg, ok := metricsConfigFromOpts(noop.NewMeterProvider().Meter("test")).(*metricsConfig)
	require.True(t, ok)

	vec, ok := cfg.GaugeVec("sessions", "node_id").(*gaugeVec)
	require.True(t, ok)

	gaugeA := vec.With(map[string]string{
		"node_id": "1",
		"az":      "a",
	})
	gaugeB := vec.With(map[string]string{
		"node_id": "1",
		"az":      "b",
	})

	require.NotSame(t, gaugeA, gaugeB)
}

func TestLabelsCacheKeyIncludesExtraLabels(t *testing.T) {
	keyA := labelsCacheKey(map[string]string{
		"node_id": "1",
		"az":      "a",
	}, []string{"node_id"})
	keyB := labelsCacheKey(map[string]string{
		"node_id": "1",
		"az":      "b",
	}, []string{"node_id"})

	require.NotEqual(t, keyA, keyB)
}

func TestDescriptorCounterUsesExactInt64Instrument(t *testing.T) {
	meter := &recordingMeter{}
	cfg := metricsConfigFromOpts(meter, WithNamespace("custom"), WithSeparator("."))
	scoped := cfg.WithSystem("topic")
	descriptorConfig, ok := scoped.(interface {
		CounterVecWithDescriptor(name, unit string, labelNames ...string) ydbmetrics.CounterVec
	})
	require.True(t, ok)

	vec := descriptorConfig.CounterVecWithDescriptor(
		"ydb.topic.reader.received.bytes", "By", "endpoint", "database",
	)
	counter := vec.With(map[string]string{"endpoint": "node", "database": "/db"})
	adder, ok := counter.(interface{ Add(delta int64) })
	require.True(t, ok)
	largeDelta := (int64(1) << 53) + 1
	adder.Add(largeDelta)
	adder.Add(1 << 20)
	counter.Inc()

	require.Len(t, meter.int64Counters, 1)
	instrument := meter.int64Counters[0]
	require.Equal(t, "ydb.topic.reader.received.bytes", instrument.name)
	require.Equal(t, "By", instrument.unit)
	require.Equal(t, []int64{largeDelta, 1 << 20, 1}, instrument.adds)
	require.Equal(t, []attribute.KeyValue{
		attribute.String("database", "/db"),
		attribute.String("endpoint", "node"),
	}, instrument.attrs[0])
}

func TestLegacyCounterUsesInt64Instrument(t *testing.T) {
	meter := &recordingMeter{}
	cfg := metricsConfigFromOpts(meter, WithNamespace("custom"), WithSeparator("."))
	scoped, ok := cfg.WithSystem("topic").(*metricsConfig)
	require.True(t, ok)

	vec := scoped.CounterVec("requests", "status")
	vec.With(map[string]string{"status": "ok"}).Inc()

	require.Len(t, meter.int64Counters, 1)
	require.Equal(t, "custom.topic.requests", meter.int64Counters[0].name)
	require.Empty(t, meter.int64Counters[0].unit)
	require.Equal(t, []int64{1}, meter.int64Counters[0].adds)
}

func TestDescriptorCacheSharesEquivalentInstruments(t *testing.T) {
	meter := &recordingMeter{}
	cfg, ok := metricsConfigFromOpts(meter).(*metricsConfig)
	require.True(t, ok)

	legacy := cfg.CounterVec("same", "label")
	require.Same(t, legacy, cfg.CounterVec("same", "label"))

	descriptorEmpty := cfg.CounterVecWithDescriptor("same", "", "label")
	require.Same(t, descriptorEmpty, cfg.CounterVecWithDescriptor("same", "", "label"))
	require.Same(t, legacy, descriptorEmpty)

	descriptorBy := cfg.CounterVecWithDescriptor("same", "By", "label")
	require.NotSame(t, descriptorEmpty, descriptorBy)
	require.Len(t, meter.int64Counters, 2)

	legacyGauge := cfg.GaugeVec("gauge", "label")
	require.Same(t, legacyGauge, cfg.GaugeVec("gauge", "label"))
	descriptorGaugeEmpty := cfg.GaugeVecWithDescriptor("gauge", "", "label")
	require.Same(t, descriptorGaugeEmpty, cfg.GaugeVecWithDescriptor("gauge", "", "label"))
	require.Same(t, legacyGauge, descriptorGaugeEmpty)
	labels := map[string]string{"label": "value"}
	legacyGauge.With(labels).Set(10)
	descriptorGaugeEmpty.With(labels).Set(20)
	require.Equal(t, []float64{10, 10}, measurementValues(meter.upDownCounters[0].adds))
	require.NotSame(t, descriptorGaugeEmpty, cfg.GaugeVecWithDescriptor("gauge", "By", "label"))
}

func TestDescriptorGaugeUsesSignedFloat64Adds(t *testing.T) {
	meter := &recordingMeter{}
	cfg := metricsConfigFromOpts(meter, WithNamespace("custom"), WithSeparator("."))
	scoped := cfg.WithSystem("topic")
	descriptorConfig, ok := scoped.(interface {
		GaugeVecWithDescriptor(name, unit string, labelNames ...string) ydbmetrics.GaugeVec
	})
	require.True(t, ok)
	vec := descriptorConfig.GaugeVecWithDescriptor(
		"ydb.topic.reader.credit_balance_bytes", "By", "endpoint",
	)

	vec.With(map[string]string{"endpoint": "node"}).Add(5)
	vec.With(map[string]string{"endpoint": "node"}).Add(-2)

	require.Len(t, meter.upDownCounters, 1)
	instrument := meter.upDownCounters[0]
	require.Equal(t, "ydb.topic.reader.credit_balance_bytes", instrument.name)
	require.Equal(t, "By", instrument.unit)
	require.Equal(t, []float64{5, -2}, measurementValues(instrument.adds))
	require.Equal(t, []attribute.KeyValue{attribute.String("endpoint", "node")}, instrument.adds[0].attrs)
}

type recordingMeter struct {
	noop.Meter

	int64Counters  []*recordingInt64Counter
	upDownCounters []*recordingFloat64UpDownCounter
}

func (m *recordingMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	config := metric.NewInt64CounterConfig(options...)
	counter := &recordingInt64Counter{name: name, unit: config.Unit()}
	m.int64Counters = append(m.int64Counters, counter)

	return counter, nil
}

func (m *recordingMeter) Float64UpDownCounter(
	name string,
	options ...metric.Float64UpDownCounterOption,
) (metric.Float64UpDownCounter, error) {
	config := metric.NewFloat64UpDownCounterConfig(options...)
	counter := &recordingFloat64UpDownCounter{name: name, unit: config.Unit()}
	m.upDownCounters = append(m.upDownCounters, counter)

	return counter, nil
}

type recordingInt64Counter struct {
	noop.Int64Counter

	name  string
	unit  string
	adds  []int64
	attrs [][]attribute.KeyValue
}

func (c *recordingInt64Counter) Add(_ context.Context, value int64, options ...metric.AddOption) {
	c.adds = append(c.adds, value)
	attrs := metric.NewAddConfig(options).Attributes()
	c.attrs = append(c.attrs, attrs.ToSlice())
}

type recordingMeasurement struct {
	value float64
	attrs []attribute.KeyValue
}

type recordingFloat64UpDownCounter struct {
	noop.Float64UpDownCounter

	name string
	unit string
	adds []recordingMeasurement
}

func (c *recordingFloat64UpDownCounter) Add(_ context.Context, value float64, options ...metric.AddOption) {
	attrs := metric.NewAddConfig(options).Attributes()
	c.adds = append(c.adds, recordingMeasurement{value: value, attrs: attrs.ToSlice()})
}

func measurementValues(measurements []recordingMeasurement) []float64 {
	values := make([]float64, 0, len(measurements))
	for _, measurement := range measurements {
		values = append(values, measurement.value)
	}

	return values
}
