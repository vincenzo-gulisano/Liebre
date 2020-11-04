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
 * Time-based, stateful window of an aggregate function. The important functions which need to be
 * implemented and define the aggregation logic are {@link TimeWindowAddRemove#add(RichTuple)},
 * {@link TimeWindowAddRemove#remove(RichTuple)} and {@link TimeWindowAddRemove#getAggregatedResult()}.
 * The actual placement of tuples in the windows is done automatically by {@link
 * TimeBasedSingleWindowAggregateOld}.
 *
 * @param <IN> The type of the input tuples.
 * @param <OUT> The type of the output tuples.
 */
public interface TimeWindowAddRemove<IN extends RichTuple, OUT extends RichTuple> extends
    ComponentFunction, TimeWindow<IN, OUT> {

  /**
   * Generate a new {@link TimeWindowAddSlide} with the same configuration and probably a clear
   * state.
   *
   * @return A new {@link TimeWindowAddSlide} instance.
   */
  TimeWindowAddRemove<IN, OUT> factory();


  /**
   * Called when a tuple is no longer a part of the window. The state of the window can be updated.
   *
   * @param t The tuple that is removed from the window.
   */
  void remove(IN t);

  /**
   * Setter for the timestamp of the earliest tuple in this window.
   *
   * @param startTimestamp The timestamp of the earliest tuple in the window.
   */
  public void setStartTimestamp(long startTimestamp);

}
