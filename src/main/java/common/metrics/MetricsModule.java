package common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class MetricsModule extends AbstractModule {

  @Override
  public void configure() {
    bind(MetricRegistry.class).in(Singleton.class);
  }
}
