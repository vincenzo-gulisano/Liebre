package experimental.provenance;

import component.operator.in1.aggregate.TimeWindowAdd;

public class GenealogTimeWindowAdd<IN extends GenealogTuple, OUT extends GenealogTuple>
    implements TimeWindowAdd<IN, OUT> {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();
  private final TimeWindowAdd<IN, OUT> window;
  private GenealogTuple first;
  private GenealogTuple last;

  public GenealogTimeWindowAdd(TimeWindowAdd<IN, OUT> window) {
    this.window = window;
  }

  @Override
  public void add(IN tuple) {
    window.add(tuple);
    addProvenanceTuple(tuple);
  }

  @Override
  public TimeWindowAdd<IN, OUT> factory() {
    return new GenealogTimeWindowAdd<>(window.factory());
  }

  @Override
  public OUT getAggregatedResult() {
    OUT result = window.getAggregatedResult();
    annotateResult(result);
    return result;
  }

  @Override
  public void setKey(String key) {
    window.setKey(key);
  }

  @Override
  public void setStartTimestamp(long startTimestamp) {
    window.setStartTimestamp(startTimestamp);
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

  protected TimeWindowAdd<IN, OUT> getWindow() {
    return window;
  }

  protected void addProvenanceTuple(IN tuple) {
    if (last == null || first == null) {
      first = tuple;
      last = tuple;
    } else {
      last.N = tuple;
      last = tuple;
    }
  }

  protected void annotateResult(OUT result) {
    if (result == null) {
      return;
    }
    result.U1 = last;
    result.U2 = first;
    result.type = GenealogTupleType.AGGREGATE;
    result.setUID(UID.increaseAndGet());
  }
}
