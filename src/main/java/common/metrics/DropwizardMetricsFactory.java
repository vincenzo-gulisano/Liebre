package common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;

public class DropwizardMetricsFactory implements MetricsFactory {

  @Inject
  private MetricName metricName;

  @Inject
  private MetricRegistry metricRegistry;

  @Override
  public Metric newAverageMetric(String id, Object type) {
    return new DropwizardAverageMetric(metricName.get(id, type), metricRegistry);
  }

  @Override
  public Metric newCountMetric(String id, Object type) {
    return new DropwizardCountMetric(metricName.get(id, type), metricRegistry);
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return new DelegatingTimeMetric(newAverageMetric(id, type));
  }


}
