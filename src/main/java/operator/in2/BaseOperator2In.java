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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

public abstract class BaseOperator2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		implements Operator2In<IN, IN2, OUT> {

	protected StreamConsumer<OUT> next;
	protected Stream<IN> input1;
	protected Stream<IN2> input2;
	protected boolean active = false;
	protected final String id;

	protected final StreamFactory streamFactory;

	public BaseOperator2In(String id, StreamFactory streamFactory) {
		this.id = id;
		this.streamFactory = streamFactory;
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		if (active) {
			throw new IllegalStateException();
		}
		if (!in.getNext().contains(this)) {
			throw new UnsupportedOperationException("Please use registerOut() to construct query graphs");
		}
		this.input1 = streamFactory.newStream(in.getId(), id);
	}

	@Override
	public void registerIn2(StreamProducer<IN2> in) {
		if (in == null) {
			return;
		}
		if (active) {
			throw new IllegalStateException();
		}
		this.input2 = streamFactory.newStream(in.getId(), id);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return Arrays.asList(this.next);
	}

	@Override
	public Stream<OUT> getOutputStream(String reqId) {
		return next.getInputStream(reqId);
	}

	@Override
	public void registerOut(StreamConsumer<OUT> out) {
		this.next = out;
		next.registerIn(this);
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return input1;
	}

	@Override
	public Stream<IN2> getInput2Stream(String reqId) {
		return input2;
	}

	@Override
	public void run() {
		if (active) {
			process();
		}
	}

	@Override
	public void activate() {
		if (input1 == null || input2 == null || next == null) {
			throw new IllegalStateException(id);
		}
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
		IN inTuple1 = getInputStream(id).getNextTuple();
		if (inTuple1 != null) {
			List<OUT> outTuples = processTupleIn1(inTuple1);
			if (outTuples != null) {
				for (OUT t : outTuples)
					getOutputStream(id).addTuple(t);
			}
		}
		IN2 inTuple2 = getInput2Stream(id).getNextTuple();
		if (inTuple2 != null) {
			List<OUT> outTuples = processTupleIn2(inTuple2);
			if (outTuples != null) {
				for (OUT t : outTuples)
					getOutputStream(id).addTuple(t);
			}
		}
	}

	@Override
	public long getPriority() {
		long in1Size = getInputStream(id) != null ? getInputStream(id).size() : 0;
		long in2Size = getInput2Stream(id) != null ? getInput2Stream(id).size() : 0;
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
