package io.palyvos.haren.function;

import io.palyvos.haren.Features;
import io.palyvos.haren.Task;

public class NoopIntraThreadSchedulingFunction extends AbstractIntraThreadSchedulingFunction {

  public static final SingleIntraThreadSchedulingFunction INSTANCE =
      new NoopIntraThreadSchedulingFunction("NO-OP");

  private NoopIntraThreadSchedulingFunction(String name) {
    super(name, Features.COMPONENT_TYPE);
  }

  @Override
  public double apply(Task task, double[][] features) {
    return 0;
  }
}
