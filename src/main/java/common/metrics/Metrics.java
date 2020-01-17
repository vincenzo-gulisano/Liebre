package common.metrics;

import com.codahale.metrics.MetricRegistry;

public final class Metrics {

  private static final MetricRegistry dropwizardMetricRegistry = new MetricRegistry();
  private static MetricName metricName = new DefaultMetricName();

  public static MetricRegistry metricRegistry() {
    return dropwizardMetricRegistry;
  }

  public static void setMetricName(MetricName metricName) {
    Metrics.metricName = metricName;
  }

  public static MetricsFactory newFileMetricsFactory(String folder, boolean autoFlush) {
    return new FileMetricsFactory(folder, metricName, autoFlush);
  }

  public static MetricsFactory newFileMetricsFactory(String folder) {
    return newFileMetricsFactory(folder, true);
  }

  public static MetricsFactory newDropWizardMetricsFactory() {
    return new DropwizardMetricsFactory(dropwizardMetricRegistry, metricName);
  }

  private Metrics() {}
}
