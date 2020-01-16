package common.metrics;

public class InactiveMetricsFactory implements MetricsFactory {

  @Override
  public Metric newAverageMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }

  @Override
  public Metric newCountMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return InactiveMetric.INSTANCE;
  }
}
