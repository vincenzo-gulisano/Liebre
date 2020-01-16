package common.metrics;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class FileMetricsFactory implements MetricsFactory {
  @Inject private MetricName metricName;

  @Inject
  @Named("metricsFolder")
  private String folder;
  @Inject
  @Named("metricsAutoFlush")
  private boolean autoFlush;

  @Override
  public Metric newAverageMetric(String id, Object type) {
    return new FileAverageMetric(metricName.get(id, type), folder, autoFlush);
  }

  @Override
  public Metric newCountMetric(String id, Object type) {
    return new FileCountMetric(metricName.get(id, type), folder, autoFlush);
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return new DelegatingTimeMetric(newAverageMetric(id, type));
  }
}
