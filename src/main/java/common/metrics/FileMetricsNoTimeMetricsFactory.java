package common.metrics;

public class FileMetricsNoTimeMetricsFactory extends FileMetricsFactory {

  public FileMetricsNoTimeMetricsFactory(String folder, MetricName metricName, boolean autoFlush) {
    super(folder, metricName, autoFlush);
  }

  public FileMetricsNoTimeMetricsFactory(String folder, MetricName metricName) {
    super(folder, metricName);
  }

  public FileMetricsNoTimeMetricsFactory(String folder) {
    super(folder);
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }
}
