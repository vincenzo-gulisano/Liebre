package common.metrics;

public class InactiveMetricsFactory implements MetricsFactory {

  @Override
  public Metric newAverageMetric(String id, Object type) {
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
  public Metric newMaxPerSecondMetric(String id, Object type) {
    throw new UnsupportedOperationException("Unimplemented method 'newMaxPerSecondMetric'");
  }

  @Override
  public Metric newTotalMaxMetric(String id, Object type) {
    throw new UnsupportedOperationException("Unimplemented method 'newTotalMaxMetric'");
  }
}
