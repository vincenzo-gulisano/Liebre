package example;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.palyvos.dcs.common.metrics.FileMetricsFactory;
import io.palyvos.dcs.common.metrics.MetricsFactory;
import query.LiebreModule;

public class ExampleModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MetricsFactory.class)
        .annotatedWith(Names.named("stream"))
        .to(FileMetricsFactory.class)
        .asEagerSingleton();
    bind(MetricsFactory.class)
        .annotatedWith(Names.named("operator"))
        .to(FileMetricsFactory.class)
        .asEagerSingleton();
    bind(MetricsFactory.class)
        .annotatedWith(Names.named("user"))
        .to(FileMetricsFactory.class)
        .asEagerSingleton();
    bindConstant().annotatedWith(Names.named("metricsFolder")).to("report");
    bindConstant().annotatedWith(Names.named("metricsAutoFlush")).to(true);
    install(new LiebreModule());
  }
}
