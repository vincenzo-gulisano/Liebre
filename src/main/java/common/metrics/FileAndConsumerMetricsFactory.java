package common.metrics;

import java.util.HashMap;
import java.util.function.Consumer;

public class FileAndConsumerMetricsFactory implements MetricsFactory {
  private final MetricName metricName;
  private final String folder;
  private final boolean autoFlush;
  private final HashMap<String, Consumer<Object[]>> consumers;

  public FileAndConsumerMetricsFactory(String folder, MetricName metricName, boolean autoFlush,
      HashMap<String, Consumer<Object[]>> consumers) {
    this.metricName = metricName;
    this.folder = folder;
    this.autoFlush = autoFlush;
    this.consumers = consumers;
  }

  public FileAndConsumerMetricsFactory(String folder, MetricName metricName,
      HashMap<String, Consumer<Object[]>> consumers) {
    this(folder, metricName, true, consumers);
  }

  public FileAndConsumerMetricsFactory(String folder,
      HashMap<String, Consumer<Object[]>> consumers) {
    this(folder, new DefaultMetricName(), true, consumers);
  }

  @Override
  public Metric newAverageMetric(String id, Object type) {
    return new FileAndConsumerAverageMetric(metricName.get(id, type), folder, autoFlush, consumers.get(id));
  }

  @Override
  public Metric newCountPerSecondMetric(String id, Object type) {
    return new FileAndConsumerCountMetric(metricName.get(id, type), folder, autoFlush, true, consumers.get(id));
  }

  @Override
  public Metric newTotalCountMetric(String id, Object type) {
    return new FileAndConsumerCountMetric(metricName.get(id, type), folder, autoFlush, false, consumers.get(id));
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
    return new FileAndConsumerMaxMetric(metricName.get(id, type), folder, autoFlush, true, consumers.get(id));
  }

  @Override
  public Metric newTotalMaxMetric(String id, Object type) {
    return new FileAndConsumerMaxMetric(metricName.get(id, type), folder, autoFlush, false, consumers.get(id));
  }
}
