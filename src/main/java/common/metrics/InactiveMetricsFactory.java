package common.metrics;

import com.codahale.metrics.Gauge;

public class InactiveMetricsFactory implements MetricsFactory {

  @Override
  public Metric newSamplingHistogramMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }

  @Override
  public Metric newCountPerSecondMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }

  @Override
  public Metric newTotalCountMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }

  @Override
  public Metric newStreamMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }

  @Override
  public Metric newGaugeMetric(String id, Object type, Gauge<Long> gauge) {
    return InactiveMetric.INSTANCE;
  }
}
