package io.palyvos.haren.function;

import io.palyvos.haren.Features;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;

public class NoopIntraThreadSchedulingFunction extends AbstractIntraThreadSchedulingFunction {

  public static final SingleIntraThreadSchedulingFunction INSTANCE =
      new NoopIntraThreadSchedulingFunction("NO-OP");

  private NoopIntraThreadSchedulingFunction(String name) {
    super(name, Features.COMPONENT_TYPE);
  }

  @Override
  public double apply(Task task, TaskIndexer indexer, double[][] features) {
    return 0;
  }

  @Override
  public void reset(int nTasks) {
  }
}
