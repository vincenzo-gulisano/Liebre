package experimental.provenance;

import component.source.SourceFunction;

public class GenealogSourceFunction<T extends GenealogTuple> implements SourceFunction<T> {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();
  private final SourceFunction<?> function;

  public GenealogSourceFunction(SourceFunction<?> function) {
    this.function = function;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get() {
    Object value = function.get();
    if (value == null) {
      return null;
    }
    if (!(value instanceof GenealogTuple)) {
      throw new IllegalArgumentException(
          String.format("Source emitted a non-provenance tuple: %s", value.getClass().getName()));
    }
    T tuple = (T) value;
    tuple.type = GenealogTupleType.SOURCE;
    if (tuple.getUID() == null) {
      tuple.setUID(UID.increaseAndGet());
    }
    return tuple;
  }

  @Override
  public double getHeadArrivalTime() {
    return function.getHeadArrivalTime();
  }

  @Override
  public double getAverageArrivalTime() {
    return function.getAverageArrivalTime();
  }

  @Override
  public boolean isInputFinished() {
    return function.isInputFinished();
  }

  @Override
  public boolean canRun() {
    return function.canRun();
  }

  @Override
  public void enable() {
    function.enable();
  }

  @Override
  public boolean isEnabled() {
    return function.isEnabled();
  }

  @Override
  public void disable() {
    function.disable();
  }
}
