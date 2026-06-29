package experimental.provenance;

import component.operator.in1.aggregate.TimeWindowAddSlide;

public class GenealogTimeWindowAddSlide<IN extends GenealogTuple, OUT extends GenealogTuple>
    implements TimeWindowAddSlide<IN, OUT> {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();
  private final TimeWindowAddSlide<IN, OUT> window;
  private GenealogTuple first;
  private GenealogTuple last;

  public GenealogTimeWindowAddSlide(TimeWindowAddSlide<IN, OUT> window) {
    this.window = window;
  }

  @Override
  public void add(IN tuple) {
    window.add(tuple);
    if (last == null || first == null) {
      first = tuple;
      last = tuple;
    } else {
      last.N = tuple;
      last = tuple;
    }
  }

  @Override
  public TimeWindowAddSlide<IN, OUT> factory() {
    return new GenealogTimeWindowAddSlide<>(window.factory());
  }

  @Override
  public void slideTo(long startTimestamp) {
    window.slideTo(startTimestamp);
    while (first != null && first.getTimestamp() < startTimestamp) {
      first = first.N;
    }
    if (first == null) {
      last = null;
    }
  }

  @Override
  public boolean isEmpty() {
    return window.isEmpty();
  }

  @Override
  public OUT getAggregatedResult() {
    OUT result = window.getAggregatedResult();
    if (result != null) {
      result.U1 = last;
      result.U2 = first;
      result.type = GenealogTupleType.AGGREGATE;
      result.setUID(UID.increaseAndGet());
    }
    return result;
  }

  @Override
  public void setKey(String key) {
    window.setKey(key);
  }

  @Override
  public void setInstanceNumber(int instanceNumber) {
    window.setInstanceNumber(instanceNumber);
  }

  @Override
  public void setParallelismDegree(int parallelismDegree) {
    window.setParallelismDegree(parallelismDegree);
  }

  @Override
  public boolean canRun() {
    return window.canRun();
  }

  @Override
  public void enable() {
    window.enable();
  }

  @Override
  public boolean isEnabled() {
    return window.isEnabled();
  }

  @Override
  public void disable() {
    window.disable();
  }
}
