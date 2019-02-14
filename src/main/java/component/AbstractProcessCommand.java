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
  private static final double EMA_ALPHA = 0.1;
  protected final T component;

  protected volatile long tuplesWritten;
  protected volatile long tuplesRead;
  protected volatile long processingTimeNanos;

  private volatile double selectivity = 1;
  private volatile double cost = 1;

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
  public void runFor(int rounds) {
    int executions = 0;
    long startTime = System.nanoTime();
    while (component.isEnabled() && executions < rounds) {
      run();
      executions += 1;
    }
    long endTime = System.nanoTime();
    // Update processing time
    processingTimeNanos += (endTime - startTime);
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
    double currentSelectivity = tuplesWritten / (double) tuplesRead;
    double currentCost = processingTimeNanos / (double) tuplesRead;
    this.selectivity = (EMA_ALPHA * currentSelectivity) + ((1 - EMA_ALPHA) * selectivity);
    this.cost = (EMA_ALPHA * currentCost) + ((1 - EMA_ALPHA) * cost);
    this.tuplesRead = this.tuplesWritten = this.processingTimeNanos = 0;
  }

  public final double getSelectivity() {
    return selectivity;
  }

  public final double getCost() {
    return cost;
  }

  @Override
  public abstract void process();

}