/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package component;

/**
 * Encapsulation of the basic execution logic for all {@link Component}s. This is required in order
 * to have reusable decorators without the need to duplicate code (i.e. the
 * process() function) and without having to resort to method interceptors.
 * <br/>
 *
 * @param <T> The component.operator subclass used.
 * @author palivosd
 */
public abstract class AbstractProcessCommand<T extends Component> implements ProcessCommand {

  // Exponential moving average alpha parameter
  // for cost and selectivity moving averages
  private static final double EMA_ALPHA = 0.5;
  protected final T component;

  private volatile long tuplesWritten;
  private volatile long tuplesRead;
  private volatile long processingTimeNanos;
  private volatile long lastUpdateTime;

  private volatile double selectivity = 1;
  private volatile double cost = 1;
  private volatile double rate = 0;

  protected AbstractProcessCommand(T component) {
    this.component = component;
  }

  @Override
  public final void run() {
    if (component.isEnabled()) {
      process();
    }
  }

  @Override
  public boolean runFor(int rounds) {
    int executions = 0;
    long tuplesWrittenBefore = tuplesWritten;
    long tuplesReadBefore = tuplesRead;
    long startTime = System.nanoTime();
    while (component.isEnabled() && executions < rounds) {
      run();
      executions += 1;
    }
    long endTime = System.nanoTime();
    // Update processing time
    processingTimeNanos += (endTime - startTime);
    return tuplesReadBefore != tuplesRead || tuplesWrittenBefore != tuplesWritten;
  }

  protected final void increaseTuplesRead() {
    tuplesRead++;
  }

  protected final void increaseTuplesWritten() {
    tuplesWritten++;
  }

  /**
   * Update the cost and selectivity based on the tuples processed and the time it took.
   * <br/>
   * <b>WARNING: The variables for the metrics are available only the execution happens with
   * {@link #runFor(int)}
   * !</b> <br/>
   * <b>WARNING: This is not thread safe! It should either be run from the operator thread or
   * from another thread while the operator is stopped. The results are visible to all threads.</b>
   */
  public final void updateMetrics() {
    if (tuplesRead == 0 || processingTimeNanos == 0) {
      return;
    }
    final long currentTime = System.currentTimeMillis();
    final double currentSelectivity = tuplesWritten / (double) tuplesRead;
    final double currentCost = processingTimeNanos / (double) tuplesRead;
    final double currentThroughput = tuplesRead / (currentTime - lastUpdateTime);
    this.selectivity = (EMA_ALPHA * currentSelectivity) + ((1 - EMA_ALPHA) * selectivity);
    this.cost = (EMA_ALPHA * currentCost) + ((1 - EMA_ALPHA) * cost);
    this.rate = currentThroughput;
    this.tuplesRead = this.tuplesWritten = this.processingTimeNanos = 0;
    this.lastUpdateTime = currentTime;
  }

  public final double getSelectivity() {
    return selectivity;
  }

  public final double getCost() {
    return cost;
  }

  public final double getRate() {
    return rate;
  }

  @Override
  public abstract void process();

}
