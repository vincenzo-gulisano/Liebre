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

  public static MetricsFactory file(String folder, boolean autoFlush) {
    return new FileMetricsFactory(folder, metricName, autoFlush);
  }

  public static MetricsFactory file(String folder) {
    return file(folder, true);
  }

  public static MetricsFactory dropWizard() {
    return new DropwizardMetricsFactory(dropwizardMetricRegistry, metricName);
  }

  private Metrics() {}
}
