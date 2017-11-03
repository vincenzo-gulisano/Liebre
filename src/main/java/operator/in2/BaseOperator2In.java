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

import java.util.Collection;
import java.util.List;

import common.BoxState;
import common.BoxState.BoxType;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

public abstract class BaseOperator2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		implements Operator2In<IN, IN2, OUT> {

	protected final BoxState<Tuple, OUT> state;
	private final String INPUT1_KEY = "INPUT1";
	private final String INPUT2_KEY = "INPUT2";
	private final String OUTPUT_KEY = "OUTPUT";

	public BaseOperator2In(String id, StreamFactory streamFactory) {
		state = new BoxState<>(id, BoxType.OPERATOR2IN, streamFactory);
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		state.setInput(INPUT1_KEY, (StreamProducer<Tuple>) in, this);
	}

	@Override
	public void registerIn2(StreamProducer<IN2> in) {
		state.setInput(INPUT2_KEY, (StreamProducer<Tuple>) in, this);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return state.getNext();
	}

	@Override
	public Collection<StreamProducer<? extends Tuple>> getPrevious() {
		return state.getPrevious();
	}

	@Override
	public Stream<OUT> getOutputStream(String reqId) {
		return state.getOutputStream(OUTPUT_KEY, this);
	}

	@Override
	public void registerOut(StreamConsumer<OUT> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return (Stream<IN>) state.getInputStream(INPUT1_KEY);
	}

	@Override
	public Stream<IN2> getInput2Stream(String reqId) {
		return (Stream<IN2>) state.getInputStream(INPUT2_KEY);
	}

	@Override
	public void run() {
		if (state.isEnabled()) {
			process();
		}
	}

	@Override
	public void activate() {
		// FIXME: Should check if both inputs are set
		state.enable();
	}

	@Override
	public void deActivate() {
		state.disable();
	}

	@Override
	public boolean isActive() {
		return state.isEnabled();
	}

	public void process() {
		IN inTuple1 = getInputStream(getId()).getNextTuple();
		if (inTuple1 != null) {
			List<OUT> outTuples = processTupleIn1(inTuple1);
			if (outTuples != null) {
				for (OUT t : outTuples)
					getOutputStream(getId()).addTuple(t);
			}
		}
		IN2 inTuple2 = getInput2Stream(getId()).getNextTuple();
		if (inTuple2 != null) {
			List<OUT> outTuples = processTupleIn2(inTuple2);
			if (outTuples != null) {
				for (OUT t : outTuples)
					getOutputStream(getId()).addTuple(t);
			}
		}
	}

	@Override
	public String getId() {
		return state.getId();
	}

	@Override
	public String toString() {
		return String.format("OP-%s", getId());
	}

	public abstract List<OUT> processTupleIn1(IN tuple);

	public abstract List<OUT> processTupleIn2(IN2 tuple);
}
