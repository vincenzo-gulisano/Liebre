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

import component.ComponentType;
import java.util.List;
import stream.Stream;

/**
 * Base abstract {@link Operator2In} implementation.
 *
 * @param <IN> The type of the tuples in the first input.
 * @param <IN2> The type of the tuples in the second input.
 * @param <OUT> The type of the tuples in the output.
 */
public abstract class BaseOperator2In<IN, IN2, OUT> extends AbstractOperator2In<IN, IN2, OUT>
     {

  /**
   * Construct.
   *
   * @param id The unique ID of this component.operator.
   */
  public BaseOperator2In(String id) {
    super(id, ComponentType.OPERATOR2IN);
  }

  @Override
  protected final void process() {
    Stream<IN> input1 = getInput();
    Stream<IN2> input2 = getInput2();
    Stream<OUT> output = getOutput();

    IN inTuple1 = input1.getNextTuple(getIndex());
    IN2 inTuple2 = input2.getNextTuple(getIndex());
    if (inTuple1 != null) {
      increaseTuplesRead();
      List<OUT> outTuples = processTupleIn1(inTuple1);
      if (outTuples != null) {
        for (OUT t : outTuples) {
          increaseTuplesWritten();
          output.addTuple(t, getIndex());
        }
      }
    }
    if (inTuple2 != null) {
      increaseTuplesRead();
      List<OUT> outTuples = processTupleIn2(inTuple2);
      if (outTuples != null) {
        for (OUT t : outTuples) {
          increaseTuplesWritten();
          output.addTuple(t, getIndex());
        }
      }
    }
  }




}
