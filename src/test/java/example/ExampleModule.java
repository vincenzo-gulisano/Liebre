package example;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.palyvos.dcs.common.metrics.DefaultMetricName;
import io.palyvos.dcs.common.metrics.FileMetricsFactory;
import io.palyvos.dcs.common.metrics.FileMetricsNoTimeMetricsFactory;
import io.palyvos.dcs.common.metrics.MetricName;
import io.palyvos.dcs.common.metrics.MetricsFactory;
import query.LiebreContext;

public class ExampleModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MetricsFactory.class)
        .annotatedWith(Names.named("stream"))
        .to(FileMetricsFactory.class)
        .asEagerSingleton();
    bind(MetricsFactory.class)
        .annotatedWith(Names.named("operator"))
        .to(FileMetricsNoTimeMetricsFactory.class)
        .asEagerSingleton();
    bind(MetricsFactory.class)
        .annotatedWith(Names.named("user"))
        .to(FileMetricsFactory.class)
        .asEagerSingleton();
    bind(MetricName.class).to(DefaultMetricName.class);
    bind(MetricRegistry.class).in(Singleton.class);
    bindConstant().annotatedWith(Names.named("metricsFolder")).to("test");
    bindConstant().annotatedWith(Names.named("metricsAutoFlush")).to(true);
    requestStaticInjection(LiebreContext.class);
  }
}
