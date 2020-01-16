package query;

import com.google.inject.AbstractModule;
import common.metrics.DefaultMetricName;
import common.metrics.MetricName;
import common.metrics.MetricsModule;

public class LiebreModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new MetricsModule());
    bind(MetricName.class).to(DefaultMetricName.class);
    requestStaticInjection(LiebreContext.class);
  }
}
