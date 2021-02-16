package common.metrics;


import com.codahale.metrics.Gauge;

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
  public Metric newSamplingHistogramMetric(String id, Object type) {
    return new FileAverageMetric(metricName.get(id, type), folder, autoFlush);
  }

  @Override
  public Metric newCountPerSecondMetric(String id, Object type) {
    return new FileCountMetric(metricName.get(id, type), folder, autoFlush);
  }

  @Override
  public Metric newTotalCountMetric(String id, Object type) {
    //FIXME: Implement
    throw new UnsupportedOperationException();
  }

  @Override
  public Metric newStreamMetric(String id, Object type) {
    return newCountPerSecondMetric(id, type);
  }

  @Override
  public Metric newGaugeMetric(String id, Object type, Gauge<Long> gauge) {
    return null;
  }

  @Override
  public TimeMetric newAverageTimeMetric(String id, Object type) {
    return new DelegatingTimeMetric(newSamplingHistogramMetric(id, type));
  }
}
