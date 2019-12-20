package component;

import io.palyvos.liebre.statistics.Statistic;
import io.palyvos.liebre.statistics.TimeStatistic;
import query.LiebreContext;

public abstract class AbstractComponent<IN, OUT> implements Component {

  protected final ComponentState<IN, OUT> state;
  private int priority;

  // Exponential moving average alpha parameter
  // for cost and selectivity moving averages
  // The ALPHA we want to use for EMAs
  private static final double TARGET_ALPHA = 0.3;
  // The update period that the target alpha would be applied to, in millis
  private static final long TARGET_UPDATE_PERIOD = 1000;
  private static final long MILLIS_TO_NANOS = 1000000;
  // The actual alpha that we use, changing depending on the actual update period length
  private volatile double alpha = 0.2;
  private volatile long tuplesWritten;
  private volatile long tuplesRead;
  private volatile long processingTimeNanos;
  private volatile long lastUpdateTime = System.currentTimeMillis();
  private volatile double selectivity = 1;

  private volatile double cost = 1;
  private volatile double rate = 0;

  private final TimeStatistic executionTimeStatistic;
  private final Statistic rateStatistic;

  public AbstractComponent(String id, ComponentType type) {
    this.state = new ComponentState<>(id, type);
    this.executionTimeStatistic =
        LiebreContext.operatorStatistiscFactory().newAverageTimeStatistic(id, "EXEC");
    this.rateStatistic = LiebreContext.operatorStatistiscFactory().newCountStatistic(id, "RATE");
  }

  @Override
  public boolean runFor(int rounds) {
    int executions = 0;
    long tuplesWrittenBefore = tuplesWritten;
    long tuplesReadBefore = tuplesRead;
    long startTime = System.nanoTime();
    // Process while the component is enabled, the rounds are not finished and there is actually
    // processing happening at every execution
    while (isEnabled() && executions < rounds) {
      run();
      executions += 1;
    }
    long endTime = System.nanoTime();
    // Update processing time
    processingTimeNanos += (endTime - startTime);
    return tuplesReadBefore != tuplesRead || tuplesWrittenBefore != tuplesWritten;
  }

  @Override
  public final void run() {
    if (isEnabled()) {
      executionTimeStatistic.startInterval();
      process();
      executionTimeStatistic.stopInterval();
    }
  }

  protected abstract void process();

  protected final void increaseTuplesRead() {
    tuplesRead++;
    rateStatistic.record(1);
  }

  protected final void increaseTuplesWritten() {
    tuplesWritten++;
  }

  /**
   * Update the cost and selectivity based on the tuples processed and the time it took. <br>
   * <b>WARNING: The variables for the metrics are available only the execution happens with {@link
   * #runFor(int)} !</b> <br>
   * <b>WARNING: This is not thread safe! It should either be run from the operator thread or from
   * another thread while the operator is stopped. The results are visible to all threads.</b>
   */
  @Override
  public final void updateMetrics() {
    updateRateAndAlpha();
    updateCostAndSelectivity();
  }

  private void updateCostAndSelectivity() {
    if (tuplesRead == 0 || processingTimeNanos == 0) {
      return;
    }
    final double currentSelectivity = tuplesWritten / (double) tuplesRead;
    final double currentCost = processingTimeNanos / (double) tuplesRead;
    this.selectivity = movingAverage(currentSelectivity, selectivity);
    this.cost = movingAverage(currentCost, cost);
    this.tuplesRead = this.tuplesWritten = this.processingTimeNanos = 0;
  }

  private void updateRateAndAlpha() {
    final long currentTime = System.currentTimeMillis();
    final long updatePeriod = currentTime - lastUpdateTime;
    if (updatePeriod == 0) {
      return;
    }
    // Update alpha value
    this.alpha = Math.min(TARGET_ALPHA, TARGET_ALPHA * updatePeriod / TARGET_UPDATE_PERIOD);
    final double currentRate = tuplesRead / (double) (MILLIS_TO_NANOS * updatePeriod);
    this.rate = movingAverage(currentRate, rate);
    this.lastUpdateTime = currentTime;
  }

  private double movingAverage(double newValue, double oldValue) {
    return (alpha * newValue) + ((1 - alpha) * oldValue);
  }

  @Override
  public final double getSelectivity() {
    return selectivity;
  }

  @Override
  public final double getCost() {
    return cost;
  }

  @Override
  public final double getRate() {
    return rate;
  }

  public ComponentType getType() {
    return state.getType();
  }

  public ConnectionsNumber inputsNumber() {
    return state.inputsNumber();
  }

  public ConnectionsNumber outputsNumber() {
    return state.outputsNumber();
  }

  public void enable() {
    executionTimeStatistic.enable();
    rateStatistic.enable();
    state.enable();
  }

  public void disable() {
    state.disable();
    executionTimeStatistic.disable();
    rateStatistic.disable();
  }

  public boolean isEnabled() {
    return state.isEnabled();
  }

  public String getId() {
    return state.getId();
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public int getIndex() {
    return state.getIndex();
  }

  @Override
  public String toString() {
    return getId();
  }
}
