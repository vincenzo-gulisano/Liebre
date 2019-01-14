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

package operator.in2;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;
import stream.Stream;

public class ProcessCommand2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		extends AbstractProcessCommand<Operator2In<IN, IN2, OUT>> {

	public ProcessCommand2In(Operator2In<IN, IN2, OUT> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		Stream<IN> input1 = operator.getInput();
		Stream<IN2> input2 = operator.getInput2();
		Stream<OUT> output = operator.getOutput();

		IN inTuple1 = input1.getNextTuple();
		IN2 inTuple2 = input2.getNextTuple();
		if (inTuple1 != null) {
			List<OUT> outTuples = operator.processTupleIn1(inTuple1);
			if (outTuples != null) {
				for (OUT t : outTuples) {
					output.addTuple(t);
				}
			}
		}
		if (inTuple2 != null) {
			List<OUT> outTuples = operator.processTupleIn2(inTuple2);
			if (outTuples != null) {
				for (OUT t : outTuples) {
					output.addTuple(t);
				}
			}
		}
	}

}
