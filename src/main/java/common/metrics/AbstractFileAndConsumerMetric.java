package common.metrics;

import java.util.function.Consumer;

public abstract class AbstractFileAndConsumerMetric extends AbstractFileMetric {

  private final Consumer<Object[]> c;

  public AbstractFileAndConsumerMetric(String id, String folder, boolean autoFlush, Consumer<Object[]> c) {
    super(id, folder, autoFlush);
    this.c = c;
  }

  protected final void writeCSVLineAndConsume(Object... values) {
    writeCSVLine(values);
    c.accept(values);
  }

}
