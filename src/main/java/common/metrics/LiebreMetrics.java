package common.metrics;

import java.util.function.Supplier;

public enum LiebreMetrics {
  LATENCY {
    @Override
    void enable() {
      this.supplier = () -> new DropwizardSamplingHistorgramMetric(name(), Metrics.metricRegistry());
    }
  };

  Supplier<Metric> supplier;

  //FIXME: What about direct metrics, e.g., Gauges? Need a different way to enable/disable!
  abstract void enable();

  void disable() {
    this.supplier = () -> InactiveMetric.INSTANCE;
  }

  Metric get() {
    return supplier.get();
  }
}
