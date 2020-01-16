package common.metrics;

public class FileMetricsNoTimeMetricsFactory extends FileMetricsFactory {

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }
}
