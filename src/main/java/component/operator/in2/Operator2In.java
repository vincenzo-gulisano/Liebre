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

package component.operator.in2;

import component.operator.Operator;
import component.operator.in1.Operator1In;
import java.util.List;
import stream.Stream;

/**
 * An {@link Operator} with two inputs (of possibly different types) and one output.
 *
 * @param <IN> The type of the tuples in the first input stream.
 * @param <IN2> The type of the tuples in the second input stream.
 * @param <OUT> The type of the tuples in the output stream.
 */
public interface Operator2In<IN, IN2, OUT> extends
    Operator1In<IN, OUT> {

  /**
   * Apply a function to the input tuple, transforming it into zero or more output tuples.
   *
   * @param tuple The tuple to be processed.
   * @return A list of zero or more output tuples.
   */
  List<OUT> processTupleIn2(IN2 tuple);

  /**
   * Set the second input {@link Stream} for this component.operator.
   *
   * @param stream The stream that forms the data connection.
   */
  void addInput2(Stream<IN2> stream);

  /**
   * Get the second input {@link Stream} for this component.operator.
   *
   * @return The second input stream.
   */
  Stream<IN2> getInput2();

  /**
   * Get an adapter of this component.operator that looks like a regular {@link Operator1In} with its main
   * input being the second input of this {@link Operator2In}.
   *
   * @return An {@link Operator} instance which has {@link #getInput2()} as its input.
   * @see SecondInputOperator2InAdapter
   */
  Operator<IN2, OUT> secondInputView();

}
