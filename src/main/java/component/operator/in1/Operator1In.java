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

import component.operator.Operator;
import java.util.List;

/**
 * {@link Operator} with one input and one output. Can produce multiple output tuples for every
 * input tuples.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public interface Operator1In<IN, OUT> extends Operator<IN, OUT> {

  /**
   * Apply a function to the input tuple, transforming it into zero or more output tuples.
   *
   * @param tuple The tuple to be processed.
   * @return A list of zero or more output tuples.
   */
  List<OUT> processTupleIn1(IN tuple);
}
