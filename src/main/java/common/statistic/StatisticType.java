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

package common.statistic;

/**
 * Predefined types of component statistics that can be recorded automatically in liebre. This enum
 * is only used to provide consistent naming.
 */
public enum StatisticType {
  @Deprecated
  PROC,
  /**
   * Execution time, in nanosec, per tuple (processing time + queueing/other delays).
   *
   * @see component.operator.Operator
   * @see component.source.Source
   * @see component.sink.Sink
   */
  EXEC,
  /**
   * Number of tuples written to a {@link stream.Stream}.
   *
   * @see stream.Stream
   */
  IN,
  /**
   * Number of tuples read from a {@link stream.Stream}.
   *
   * @see stream.Stream
   */
  OUT;
}
