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

import common.statistic.MeterStatistic;
import common.tuple.Tuple;
import common.util.StatisticPath;
import common.util.StatisticType;

/**
 * Statistic recorder for {@link Stream}s. Records the statistics {@link StatisticType#IN} and
 * {@link StatisticType#OUT}.
 *
 * @param <T> The type of tuples transferred by the stream.
 */
public class StreamStatistic<T extends Tuple> extends StreamDecorator<T> {

  private final MeterStatistic inRate;
  private final MeterStatistic outRate;

  /**
   * Construct.
   *
   * @param stream The stream to record the statistics for.
   * @param outputFolder The path of the file where the statistics are written to.
   * @param autoFlush The autoflush parameter for the file writer.
   */
  public StreamStatistic(Stream<T> stream, String outputFolder, boolean autoFlush) {
    super(stream);
    inRate = new MeterStatistic(StatisticPath.get(outputFolder, stream, StatisticType.IN),
        autoFlush);
    outRate = new MeterStatistic(StatisticPath.get(outputFolder, stream, StatisticType.OUT),
        autoFlush);
  }

  @Override
  public void addTuple(T tuple,int writer) {
    inRate.append(1);
    super.addTuple(tuple,writer);
  }

  @Override
  public T getNextTuple(int reader) {
    T out = super.getNextTuple(reader);
    if (out != null) {
      outRate.append(1);
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
