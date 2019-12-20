package query;

import com.google.inject.AbstractModule;
import io.palyvos.dcs.common.metrics.DefaultMetricName;
import io.palyvos.dcs.common.metrics.MetricName;
import io.palyvos.dcs.common.metrics.MetricsModule;

public class LiebreModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new MetricsModule());
    bind(MetricName.class).to(DefaultMetricName.class);
    requestStaticInjection(LiebreContext.class);
  }
}
