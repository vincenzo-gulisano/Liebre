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

package stream;

import common.statistic.LiebreMetrics;
import io.palyvos.liebre.statistics.Statistic;
import io.palyvos.liebre.statistics.StatisticsFactory;
import common.statistic.StatisticType;

/**
 * Statistic recorder for {@link Stream}s. Records the statistics {@link StatisticType#IN} and
 * {@link StatisticType#OUT}.
 *
 * @param <T> The type of tuples transferred by the stream.
 */
public class StreamStatistic<T> extends StreamDecorator<T> {



  private StatisticsFactory statisticsFactory = LiebreMetrics.statistiscFactory();
  private final Statistic inRate;
  private final Statistic outRate;

  /**
   * Construct.
   *
   * @param stream The stream to record the statistics for.
   * @param outputFolder The path of the file where the statistics are written to.
   * @param autoFlush The autoflush parameter for the file writer.
   */
  public StreamStatistic(Stream<T> stream, String outputFolder, boolean autoFlush) {
    super(stream);
    inRate = statisticsFactory.newCountStatistic(stream.getId(), StatisticType.IN);
    outRate = statisticsFactory.newCountStatistic(stream.getId(), StatisticType.OUT);
  }

  @Override
  public void addTuple(T tuple,int writer) {
    inRate.record(1);
    super.addTuple(tuple,writer);
  }

  @Override
  public T getNextTuple(int reader) {
    T out = super.getNextTuple(reader);
    if (out != null) {
      outRate.record(1);
    }
    return out;
  }

  @Override
  public boolean offer(T tuple,int writer) {
    throw new UnsupportedOperationException("stream statistic must be the last decorator!");
  }

  @Override
  public T poll(int reader) {
    throw new UnsupportedOperationException("stream statistic must be the last decorator!");
  }

  @Override
  public void enable() {
    super.enable();
    inRate.enable();
    outRate.enable();
  }

  @Override
  public void disable() {
    inRate.disable();
    outRate.disable();
    super.disable();
  }

}
