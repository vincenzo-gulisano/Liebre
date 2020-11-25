package stream;

import query.LiebreContext;
import common.metrics.Metric;

public abstract class AbstractStream<T> implements Stream<T> {

  public static final String METRIC_IN = "IN";
  public static final String METRIC_OUT = "OUT";
  public static final String METRIC_QUEUE_SIZE = "QUEUE_SIZE";
  protected final String id;
  protected final int index;
  private final Metric queueSizeMetric;
  protected boolean enabled;

  private final Metric inMetric;
  private final Metric outMetric;

  public AbstractStream(String id, int index) {
    this.id = id;
    this.index = index;
    inMetric = LiebreContext.streamMetrics().newStreamMetric(id, METRIC_IN);
    outMetric = LiebreContext.streamMetrics().newStreamMetric(id, METRIC_OUT);
    queueSizeMetric = LiebreContext.streamMetrics().newAverageMetric(id, METRIC_QUEUE_SIZE);
  }

  @Override
  public final void addTuple(T tuple, int producerIndex) {
    doAddTuple(tuple, producerIndex);
    inMetric.record(1);
    queueSizeMetric.record(size());
  }

  @Override
  public final T getNextTuple(int consumerIndex) {
    T tuple = doGetNextTuple(consumerIndex);
    if (tuple != null) {

      outMetric.record(1);
      queueSizeMetric.record(size());
    }
    return tuple;
  }

  protected abstract T doGetNextTuple(int consumerIndex);

  protected abstract void doAddTuple(T tuple, int producerIndex);

  @Override
  public void enable() {
    inMetric.enable();
    outMetric.enable();
    queueSizeMetric.enable();
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
    queueSizeMetric.disable();
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
