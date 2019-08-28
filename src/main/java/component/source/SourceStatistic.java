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

package component.source;

import common.statistic.HistogramStatistic;
import common.tuple.Tuple;
import common.util.StatisticPath;
import common.util.StatisticType;

/**
 * Statistic decorator for {@link Source}.
 * Records, in separate CSV files, {@link StatisticType#PROC} and {@link StatisticType#EXEC}
 *
 * @see StatisticPath
 */
public class SourceStatistic<T extends Tuple> extends SourceDecorator<T> {


  private final HistogramStatistic processingTimeStatistic;
  private final HistogramStatistic executionTimeStatistic;

  /**
   * Add statistics to the given component.source.
   *
   * @param source The component.source to add statistics to
   * @param outputFolder The folder where the statistics will be saved as CSV files
   * @param autoFlush The autoFlush parameter for the file buffers
   */
  public SourceStatistic(Source<T> source, String outputFolder,
      boolean autoFlush) {
    super(source);
    this.processingTimeStatistic = new HistogramStatistic(
        StatisticPath.get(outputFolder, source, StatisticType.PROC), autoFlush);
    this.executionTimeStatistic = new HistogramStatistic(
        StatisticPath.get(outputFolder, source, StatisticType.EXEC), autoFlush);
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
  public T getNextTuple() {
    long start = System.nanoTime();
    T tuple = super.getNextTuple();
    processingTimeStatistic.append(System.nanoTime() - start);
    return tuple;
  }

  @Override
  public void run() {
    long start = System.nanoTime();
    super.run();
    executionTimeStatistic.append(System.nanoTime() - start);
  }
}
