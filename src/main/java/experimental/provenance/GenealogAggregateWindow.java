package experimental.provenance;

import component.operator.in1.aggregate.TimeWindowAddRemove;

public class GenealogAggregateWindow<IN extends GenealogTuple, OUT extends GenealogTuple>
    implements TimeWindowAddRemove<IN, OUT> {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();
  private TimeWindowAddRemove<IN, OUT> window;
  private GenealogTuple first;
  private GenealogTuple last;

  public GenealogAggregateWindow(TimeWindowAddRemove<IN, OUT> window) {
    this.window = window;
  }

  public void add(IN tuple) {
    this.window.add(tuple);
    if (this.last == null || this.first == null) {
      this.first = tuple;
      this.last = tuple;
    } else {
      this.last.N = tuple;
      this.last = tuple;
    }
  }

  public TimeWindowAddRemove<IN, OUT> factory() {
    return new GenealogAggregateWindow<IN, OUT>(window.factory());
  }

  public OUT getAggregatedResult() {
    OUT result = window.getAggregatedResult();
    result.U1 = last;
    result.U2 = first;
    result.type = GenealogTupleType.AGGREGATE;
    result.setUID(UID.increaseAndGet());
    return result;
  }

  public void remove(IN t) {
    first = t.N;
    window.remove(t);
  }

  public void setKey(String arg0) {
    window.setKey(arg0);
  }

  @Override
  public void setInstanceNumber(int instanceNumber) {

  }

  @Override
  public void setParallelismDegree(int parallelismDegree) {

  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  public void setStartTimestamp(long ts) {
    window.setStartTimestamp(ts);
  }

}
