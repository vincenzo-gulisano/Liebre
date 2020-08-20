package common.metrics;

import com.codahale.metrics.MetricRegistry;

public class DropwizardMetricsFactory implements MetricsFactory {

  private final MetricName metricName;

  private final MetricRegistry metricRegistry;

  public DropwizardMetricsFactory(MetricRegistry metricRegistry, MetricName metricName) {
    this.metricName = metricName;
    this.metricRegistry = metricRegistry;
  }

  public DropwizardMetricsFactory(MetricRegistry metricRegistry) {
    this(metricRegistry, new DefaultMetricName());
  }

  @Override
  public Metric newAverageMetric(String id, Object type) {
    return new DropwizardAverageMetric(metricName.get(id, type), metricRegistry);
  }

  @Override
  public Metric newCountPerSecondMetric(String id, Object type) {
    return new DropwizardCountPerSecondMetric(metricName.get(id, type), metricRegistry);
  }

  @Override
  public Metric newTotalCountMetric(String id, Object type) {
    return new DropwizardCountMetric(metricName.get(id, type), metricRegistry);
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return new DelegatingTimeMetric(newAverageMetric(id, type));
  }

  @Override
  public Metric newStreamMetric(String id, Object type) {
    return newTotalCountMetric(id, type);
  }
}
