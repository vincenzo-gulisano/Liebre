package stream;

import java.util.function.Function;
import query.LiebreContext;
import common.metrics.Metric;

public abstract class AbstractStream<T> implements Stream<T> {

  public static final String METRIC_IN = "IN";
  public static final String METRIC_OUT = "OUT";
  public static final String METRIC_QUEUE_SIZE = "QUEUE_SIZE";
  public static final String METRIC_ARRIVAL_TIME = "ARRIVAL_TIME";
  protected final String id;
  protected final int index;
  protected boolean enabled;

  private final Metric inMetric;
  private final Metric outMetric;
  protected volatile long tuplesRead;
  protected volatile long tuplesWritten;

  public AbstractStream(String id, int index) {
    this.id = id;
    this.index = index;
    inMetric = LiebreContext.streamMetrics().newStreamMetric(id, METRIC_IN);
    outMetric = LiebreContext.streamMetrics().newStreamMetric(id, METRIC_OUT);
    LiebreContext.streamMetrics().newGaugeMetric(id, METRIC_QUEUE_SIZE, () -> Long
        .valueOf(size()));
  }

  @Override
  public final void addTuple(T tuple, int producerIndex) {
    doAddTuple(tuple, producerIndex);
    inMetric.record(1);
  }

  @Override
  public final T getNextTuple(int consumerIndex) {
    T tuple = doGetNextTuple(consumerIndex);
    if (tuple != null) {
      outMetric.record(1);
    }
    return tuple;
  }

  protected abstract T doGetNextTuple(int consumerIndex);

  protected abstract void doAddTuple(T tuple, int producerIndex);

  @Override
  public void enable() {
    inMetric.enable();
    outMetric.enable();
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    inMetric.disable();
    outMetric.disable();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public int getIndex() {
    return index;
  }
}
