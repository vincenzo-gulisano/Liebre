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

package operator.router;

import common.statistic.AverageStatistic;
import common.tuple.Tuple;
import common.util.StatisticFilename;
import java.util.Collection;
import stream.Stream;

public class RouterOperatorStatistic<T extends Tuple> extends RouterOperatorDecorator<T> {

  private final AverageStatistic processingTimeStatistic;
  private final AverageStatistic executionTimeStatistic;

  public RouterOperatorStatistic(RouterOperator<T> operator, String outputFolder,
      boolean autoFlush) {
    super(operator);
    this.processingTimeStatistic = new AverageStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "proc"), autoFlush);
    this.executionTimeStatistic = new AverageStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "exec"), autoFlush);
  }

  public RouterOperatorStatistic(RouterOperator<T> decorated, String outputFolder) {
    this(decorated, outputFolder, true);
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
  public Collection<? extends Stream<T>> chooseOutputs(T tuple) {
    long start = System.nanoTime();
    Collection<? extends Stream<T>> chosenOutputs = super.chooseOutputs(tuple);
    processingTimeStatistic.append(System.nanoTime() - start);
    return chosenOutputs;
  }

  @Override
  public void run() {
    long start = System.nanoTime();
    super.run();
    executionTimeStatistic.append(System.nanoTime() - start);
  }
}
