package common.metrics;

import com.codahale.metrics.Gauge;

public interface MetricsFactory {

  Metric newSamplingHistogramMetric(String id, Object type);

  Metric newCountPerSecondMetric(String id, Object type);

  Metric newTotalCountMetric(String id, Object type);

  TimeMetric newAverageTimeMetric(String id, Object type);

  Metric newStreamMetric(String id, Object type);

  Metric newGaugeMetric(String id, Object type, Gauge<Long> gauge);
}
