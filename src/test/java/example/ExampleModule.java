package example;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.palyvos.liebre.statistics.DefaultStatisticName;
import io.palyvos.liebre.statistics.FileStatisticFactory;
import io.palyvos.liebre.statistics.LiebreMetrics;
import io.palyvos.liebre.statistics.StatisticFactory;
import io.palyvos.liebre.statistics.StatisticName;

public class ExampleModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(StatisticFactory.class).to(FileStatisticFactory.class).asEagerSingleton();
    bind(StatisticName.class).to(DefaultStatisticName.class);
    bindConstant().annotatedWith(Names.named("statisticsFolder")).to("test");
    bindConstant().annotatedWith(Names.named("statisticsAutoFlush")).to(false);
    requestStaticInjection(LiebreMetrics.class);
  }
}
