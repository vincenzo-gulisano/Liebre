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

package common.util;

/**
 * Predefined types of component statistics that can be recorded automatically in liebre. This
 * enum is only used to provide consistent naming, together with {@link StatisticPath}.
 *
 * @see common.statistic.AverageStatistic
 * @see common.statistic.CountStatistic
 */
public enum StatisticType {
  /**
   * Processing time, in nanosec, per tuple. Average per second.
   *
   * @see operator.Operator
   * @see source.Source
   * @see sink.Sink
   */
  PROC,
  /**
   * Execution time, in nanosec, per tuple (processing time + queueing/other delays). Average
   * per second.
   *
   * @see operator.Operator
   * @see source.Source
   * @see sink.Sink
   */
  EXEC,
  /**
   * Tuples written to a {@link stream.Stream}. Sum per second.
   *
   * @see stream.Stream
   */
  IN,
  /**
   * Tuples read from a {@link stream.Stream}. Sum per second.
   *
   * @see stream.Stream
   */
  OUT;
}
