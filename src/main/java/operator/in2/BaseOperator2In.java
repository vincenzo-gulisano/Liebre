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

package operator.in2;

import java.util.List;

import common.tuple.Tuple;
import stream.Stream;

public abstract class BaseOperator2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		implements Operator2In<IN, IN2, OUT> {

	protected Stream<IN> in1;
	protected Stream<IN2> in2;
	protected Stream<OUT> out;
	protected boolean active = false;
	private final String id;

	public BaseOperator2In(String id) {
		this.id = id;
	}

	@Override
	public void registerIn(String id, Stream<IN> in) {
		this.in1 = in;
	}

	@Override
	public void registerIn2(String id, Stream<IN2> in) {
		this.in2 = in;
	}

	@Override
	public void registerOut(String id, Stream<OUT> out) {
		this.out = out;
	}

	@Override
	public void run() {
		if (active) {
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

	@Override
	public boolean isActive() {
		return active;
	}

	public void process() {
		IN inTuple1 = in1.getNextTuple();
		if (inTuple1 != null) {
			List<OUT> outTuples = processTupleIn1(inTuple1);
			if (outTuples != null) {
				for (OUT t : outTuples)
					out.addTuple(t);
			}
		}
		IN2 inTuple2 = in2.getNextTuple();
		if (inTuple2 != null) {
			List<OUT> outTuples = processTupleIn2(inTuple2);
			if (outTuples != null) {
				for (OUT t : outTuples)
					out.addTuple(t);
			}
		}
	}

	@Override
	public long getPriority() {
		long in1Size = in1 != null ? in1.size() : 0;
		long in2Size = in2 != null ? in2.size() : 0;
		return Math.max(in1Size, in2Size);
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public String toString() {
		return String.format("OP [id=%s, priority=%d]", id, getPriority());
	}

	public abstract List<OUT> processTupleIn1(IN tuple);

	public abstract List<OUT> processTupleIn2(IN2 tuple);
}
