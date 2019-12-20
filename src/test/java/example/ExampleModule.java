package example;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.palyvos.liebre.statistics.DefaultStatisticName;
import io.palyvos.liebre.statistics.FileStatisticsFactory;
import io.palyvos.liebre.statistics.FileStatisticsNoTimeStatisticsFactory;
import io.palyvos.liebre.statistics.StatisticName;
import io.palyvos.liebre.statistics.StatisticsFactory;
import query.LiebreContext;

public class ExampleModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(StatisticsFactory.class).annotatedWith(Names.named("stream")).to(FileStatisticsFactory.class).asEagerSingleton();
    bind(StatisticsFactory.class).annotatedWith(Names.named("operator")).to(
        FileStatisticsNoTimeStatisticsFactory.class).asEagerSingleton();
    bind(StatisticName.class).to(DefaultStatisticName.class);
    bind(MetricRegistry.class).in(Singleton.class);
    bindConstant().annotatedWith(Names.named("statisticsFolder")).to("test");
    bindConstant().annotatedWith(Names.named("statisticsAutoFlush")).to(true);
    requestStaticInjection(LiebreContext.class);
  }
}
