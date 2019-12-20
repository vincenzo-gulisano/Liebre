package example;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import common.statistic.LiebreMetrics;
import io.palyvos.liebre.statistics.DefaultStatisticName;
import io.palyvos.liebre.statistics.FileStatisticsFactory;
import io.palyvos.liebre.statistics.StatisticName;
import io.palyvos.liebre.statistics.StatisticsFactory;

public class ExampleModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(StatisticsFactory.class).to(FileStatisticsFactory.class).asEagerSingleton();
    bind(StatisticName.class).to(DefaultStatisticName.class);
    bind(MetricRegistry.class).in(Singleton.class);
    bindConstant().annotatedWith(Names.named("statisticsFolder")).to("test");
    bindConstant().annotatedWith(Names.named("statisticsAutoFlush")).to(true);
    requestStaticInjection(LiebreMetrics.class);
  }
}
