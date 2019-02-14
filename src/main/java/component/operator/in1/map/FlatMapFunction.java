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

package component.operator.in1.map;

import common.Active;
import common.tuple.Tuple;
import java.util.List;

/**
 * Function that maps a tuple to zero or more output tuples.
 *
 * @param <IN> The type of the input tuple.
 * @param <OUT> The type of the output tuple(s).
 */
@FunctionalInterface
public interface FlatMapFunction<IN extends Tuple, OUT extends Tuple> extends Active {

  /**
   * Apply a function mapping the input to zero or more output tuples. If no tuples are to be
   * produced, an empty or null list is acceptable.
   *
   * @param tuple The tuple to be mapped.
   * @return The result of the function.
   */
  List<OUT> apply(IN tuple);

  @Override
  default void enable() {
  }

  @Override
  default boolean isEnabled() {
    return true;
  }

  @Override
  default void disable() {
  }

}