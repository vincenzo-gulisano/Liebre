/*  Copyright (C) 2017  Vincenzo Gulisano
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package operator2in;

import java.util.List;

import stream.Stream;
import tuple.Tuple;

public abstract class BaseOperator2In<T1 extends Tuple, T2 extends Tuple, T3 extends Tuple>
		implements Operator2In<T1, T2, T3> {

	protected Stream<T1> in1;
	protected Stream<T2> in2;
	protected Stream<T3> out;
	protected boolean active = false;

	public BaseOperator2In() {
	}

	@Override
	public void registerIn1(String id, Stream<T1> in) {
		this.in1 = in;
	}

	@Override
	public void registerIn2(String id, Stream<T2> in) {
		this.in2 = in;
	}

	@Override
	public void registerOut(String id, Stream<T3> out) {
		this.out = out;
	}

	@Override
	public void run() {
		while (active) {
			process();
		}
	}

	@Override
	public void activate() {
		active = true;
	}

	@Override
	public void deActivate() {
		active = false;
	}

	public void process() {
		T1 inTuple1 = in1.getNextTuple();
		if (inTuple1 != null) {
			List<T3> outTuples = processTupleIn1(inTuple1);
			if (outTuples != null) {
				for (T3 t : outTuples)
					out.addTuple(t);
			}
		}
		T2 inTuple2 = in2.getNextTuple();
		if (inTuple2 != null) {
			List<T3> outTuples = processTupleIn2(inTuple2);
			if (outTuples != null) {
				for (T3 t : outTuples)
					out.addTuple(t);
			}
		}
	}

	public abstract List<T3> processTupleIn1(T1 tuple);

	public abstract List<T3> processTupleIn2(T2 tuple);
}
