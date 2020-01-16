package common.metrics;

public interface MetricsFactory {

  Metric newAverageMetric(String id, Object type);

  Metric newCountMetric(String id, Object type);

  TimeMetric newAverageTimeMetric(String id, Object type);
}
