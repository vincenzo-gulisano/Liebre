package stream;

import query.LiebreContext;
import io.palyvos.liebre.statistics.Statistic;

public abstract class AbstractStream<T> implements Stream<T> {

  public static final String STATISTIC_IN = "IN";
  public static final String STATISTIC_OUT = "OUT";
  protected final String id;
  protected final int index;
  private volatile boolean enabled;

  private final Statistic inStatistic;
  private final Statistic outStatistic;

  public AbstractStream(String id, int index) {
    this.id = id;
    this.index = index;
    inStatistic = LiebreContext.streamStatisticsFactory().newCountStatistic(id, STATISTIC_IN);
    outStatistic = LiebreContext.streamStatisticsFactory().newCountStatistic(id, STATISTIC_OUT);
  }

  @Override
  public final void addTuple(T tuple, int writer) {
    doAddTuple(tuple, writer);
    inStatistic.record(1);
  }

  @Override
  public final T getNextTuple(int reader) {
    T tuple = doGetNextTuple(reader);
    if (tuple != null) {
      outStatistic.record(1);
    }
    return tuple;
  }

  protected abstract T doGetNextTuple(int reader);

  protected abstract void doAddTuple(T tuple, int writer);

  @Override
  public void enable() {
    inStatistic.enable();
    outStatistic.enable();
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    inStatistic.disable();
    outStatistic.disable();
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
