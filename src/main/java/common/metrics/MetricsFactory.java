package common.metrics;

public interface MetricsFactory {

  Metric newAverageMetric(String id, Object type);

  Metric newCountPerSecondMetric(String id, Object type);

  Metric newTotalCountMetric(String id, Object type);

  TimeMetric newAverageTimeMetric(String id, Object type);

  Metric newStreamMetric(String id, Object type);
}
