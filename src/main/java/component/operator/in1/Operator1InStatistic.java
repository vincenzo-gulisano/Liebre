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

package component.operator.in1;

import io.palyvos.liebre.statistics.LiebreMetrics;
import io.palyvos.liebre.statistics.StatisticFactory;
import io.palyvos.liebre.statistics.StatisticType;
import io.palyvos.liebre.statistics.TimeStatistic;
import java.util.List;

/**
 * Statistic decorator for {@link Operator1In}.
 * Records, in separate CSV files, {@link StatisticType#PROC} and {@link StatisticType#EXEC}.
 *
 * @see StatisticType
 * @see StatisticPath
 */
public class Operator1InStatistic<IN, OUT> extends
    Operator1InDecorator<IN, OUT> {

  private final StatisticFactory statisticFactory = LiebreMetrics.statistiscFactory();
  private final TimeStatistic processingTimeStatistic;
  private final TimeStatistic executionTimeStatistic;

  /**
   * Add statistics to the given component.operator.
   *
   * @param operator The component.operator to add statistics to
   * @param outputFolder The folder where the statistics will be saved as CSV files
   * @param autoFlush The autoFlush parameter for the file buffers
   */
  public Operator1InStatistic(Operator1In<IN, OUT> operator, String outputFolder,
      boolean autoFlush) {
    super(operator);
    this.processingTimeStatistic = statisticFactory.newAverageTimeStatistic(getId(), StatisticType.PROC);
    this.executionTimeStatistic = statisticFactory.newAverageTimeStatistic(getId(), StatisticType.EXEC);
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
    processingTimeStatistic.startInterval();
    List<OUT> outTuples = super.processTupleIn1(tuple);
    processingTimeStatistic.stopInterval();
    return outTuples;
  }

  @Override
  public void run() {
    executionTimeStatistic.startInterval();
    super.run();
    executionTimeStatistic.stopInterval();
  }
}
