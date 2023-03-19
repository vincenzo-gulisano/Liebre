package common.metrics;


public class FileMetricsFactory implements MetricsFactory {
  private final MetricName metricName;
  private final String folder;
  private final boolean autoFlush;

  public FileMetricsFactory(String folder, MetricName metricName, boolean autoFlush) {
    this.metricName = metricName;
    this.folder = folder;
    this.autoFlush = autoFlush;
  }

  public FileMetricsFactory(String folder, MetricName metricName) {
    this(folder, metricName, true);
  }

  public FileMetricsFactory(String folder) {
    this(folder, new DefaultMetricName(), true);
  }

  @Override
  public Metric newAverageMetric(String id, Object type) {
    return new FileAverageMetric(metricName.get(id, type), folder, autoFlush);
  }

  @Override
  public Metric newCountPerSecondMetric(String id, Object type) {
    return new FileCountMetric(metricName.get(id, type), folder, autoFlush, true);
  }

  @Override
  public Metric newTotalCountMetric(String id, Object type) {
    return new FileCountMetric(metricName.get(id, type), folder, autoFlush, false);
  }

  @Override
  public Metric newStreamMetric(String id, Object type) {
    return newCountPerSecondMetric(id, type);
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return new DelegatingTimeMetric(newAverageMetric(id, type));
  }

  @Override
  public Metric newMaxPerSecondMetric(String id, Object type) {
    return new FileMaxMetric(metricName.get(id, type), folder, autoFlush, true);
  }

  @Override
  public Metric newTotalMaxMetric(String id, Object type) {
    return new FileMaxMetric(metricName.get(id, type), folder, autoFlush, false);
  }
}
