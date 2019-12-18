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

import component.AbstractProcessCommand;
import java.util.List;
import stream.Stream;

/**
 * Process command implementation for {@link Operator2In}.
 *
 * @see AbstractProcessCommand
 */
class ProcessCommand2In<IN, IN2, OUT>
    extends AbstractProcessCommand<Operator2In<IN, IN2, OUT>> {

  public ProcessCommand2In(Operator2In<IN, IN2, OUT> operator) {
    super(operator);
  }

  @Override
  public final void process() {
    Stream<IN> input1 = component.getInput();
    Stream<IN2> input2 = component.getInput2();
    Stream<OUT> output = component.getOutput();

    IN inTuple1 = input1.getNextTuple(component.getRelativeConsumerIndex());
    IN2 inTuple2 = input2.getNextTuple(component.getRelativeConsumerIndex());
    if (inTuple1 != null) {
      increaseTuplesRead();
      List<OUT> outTuples = component.processTupleIn1(inTuple1);
      if (outTuples != null) {
        for (OUT t : outTuples) {
          increaseTuplesWritten();
          output.addTuple(t,component.getRelativeProducerIndex());
        }
      }
    }
    if (inTuple2 != null) {
      increaseTuplesRead();
      List<OUT> outTuples = component.processTupleIn2(inTuple2);
      if (outTuples != null) {
        for (OUT t : outTuples) {
          increaseTuplesWritten();
          output.addTuple(t,component.getRelativeProducerIndex());
        }
      }
    }
  }

}
