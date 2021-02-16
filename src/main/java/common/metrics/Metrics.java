package common.metrics;

import com.codahale.metrics.MetricRegistry;

public final class Metrics {

  public static double SAMPLE_PERCENTAGE = 0.1;
  private static final MetricRegistry dropwizardMetricRegistry = new MetricRegistry();
  private static MetricName metricName = new DefaultMetricName();

  public static MetricRegistry metricRegistry() {
    return dropwizardMetricRegistry;
  }

  public static void setMetricName(MetricName metricName) {
    Metrics.metricName = metricName;
  }

  public static MetricsFactory file(String folder, boolean autoFlush) {
    throw new UnsupportedOperationException(); //FIXME: Remove
  }

  public static MetricsFactory file(String folder) {
    return file(folder, true);
  }

  public static MetricsFactory dropWizard() {
    return new DropwizardMetricsFactory(dropwizardMetricRegistry, metricName);
  }

  public static long sampleEvery() {
    return SAMPLE_PERCENTAGE  > 0 ? Math.round(1/SAMPLE_PERCENTAGE) : -1;
  }

  private Metrics() {}
}
