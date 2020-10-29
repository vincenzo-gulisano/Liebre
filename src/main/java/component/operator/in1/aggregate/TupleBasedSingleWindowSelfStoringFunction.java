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

package component.operator.in1.aggregate;

import common.tuple.RichTuple;
import component.ComponentFunction;

/**
 * Tuple-based, stateful window of an aggregate function. The important functions which need to be
 * implemented and define the aggregation logic are {@link TupleBasedSingleWindowSelfStoringFunction#add(Object)},
 * and {@link TupleBasedSingleWindowSelfStoringFunction#getAggregatedResult()}.
 *
 * @param <IN> The type of the input tuples.
 * @param <OUT> The type of the output tuples.
 */
public interface TupleBasedSingleWindowSelfStoringFunction<IN, OUT> extends
    ComponentFunction {

  /**
   * Called when a new tuple is added to the window. The state of the window can be updated.
   *
   * @param t The new tuple that is added to the window.
   */
  void add(IN t);

  /**
   * Called when the window is slides by the given amount of tuples
   *
   * @param tuples Number of tuples to be discarded due to the window slide.
   */
  void slideBy(long tuples);

  /**
   * Called when a window must produce a result based on its current state, i.e., the tuples
   * currently present in it.
   *
   * @return The aggregation result.
   */
  OUT getAggregatedResult();

  /**
   * Setter for the instance number of the aggregate maintaining this window.
   *
   * @param instanceNumber The instance number of the aggregate maintaining this window.
   */
  void setInstanceNumber(int instanceNumber);

  /**
   * Setter for the parallelism degree of the aggregate maintaining this window.
   *
   * @param parallelismDegree The number of instances of the aggregate maintaining this window.
   */
  void setParallelismDegree(int parallelismDegree);

}
