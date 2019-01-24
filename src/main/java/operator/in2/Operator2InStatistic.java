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

package operator.in2;

import common.statistic.AverageStatistic;
import common.tuple.Tuple;
import common.util.StatisticFilename;
import java.util.List;

/**
 * Statistic decorator for {@link Operator2In}.
 * <p>Records, in separate CSV files:
 * <ul><li>The processing time per tuple: How much time to apply the operator function to the
 * tuple.
 * </li><li>The execution time per tuple: How much time it takes to fully process a tuple,
 * including any queueing or other delays.
 * </li></ul></p>
 *
 * @see StatisticFilename
 */
public class Operator2InStatistic<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
    extends Operator2InDecorator<IN, IN2, OUT> {

  private final AverageStatistic processingTimeStatistic;
  private final AverageStatistic executionTimeStatistic;

  /**
   * Add statistics to the given operator.
   *
   * @param operator The operator to add statistics to
   * @param outputFolder The folder where the statistics will be saved as CSV files
   * @param autoFlush The autoFlush parameter for the file buffers
   */
  public Operator2InStatistic(Operator2In<IN, IN2, OUT> operator, String outputFolder,
      boolean autoFlush) {
    super(operator);
    this.processingTimeStatistic = new AverageStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "proc"), autoFlush);
    this.executionTimeStatistic = new AverageStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "exec"), autoFlush);
  }

  @Override
  public void enable() {
    super.enable();
    processingTimeStatistic.enable();
    executionTimeStatistic.enable();
  }

  @Override
  public void disable() {
    processingTimeStatistic.disable();
    executionTimeStatistic.disable();
    super.disable();
  }

  @Override
  public List<OUT> processTupleIn1(IN tuple) {
    long start = System.nanoTime();
    List<OUT> outTuples = super.processTupleIn1(tuple);
    processingTimeStatistic.append(System.nanoTime() - start);
    return outTuples;
  }

  @Override
  public List<OUT> processTupleIn2(IN2 tuple) {
    long start = System.nanoTime();
    List<OUT> outTuples = super.processTupleIn2(tuple);
    processingTimeStatistic.append(System.nanoTime() - start);
    return outTuples;
  }

  @Override
  public void run() {
    long start = System.nanoTime();
    super.run();
    executionTimeStatistic.append(System.nanoTime() - start);
  }
}
