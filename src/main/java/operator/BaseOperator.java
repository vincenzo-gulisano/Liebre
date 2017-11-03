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

package operator;

import java.util.Collection;
import java.util.List;

import common.BoxState;
import common.BoxState.BoxType;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

public abstract class BaseOperator<IN extends Tuple, OUT extends Tuple> implements Operator<IN, OUT> {

	protected final BoxState<IN, OUT> state;
	private final String INPUT_KEY = "INPUT";
	private final String OUTPUT_KEY = "OUTPUT";

	protected BaseOperator(String id, BoxType type, StreamFactory streamFactory) {
		state = new BoxState<>(id, type, streamFactory);
	}

	public BaseOperator(String id, StreamFactory streamFactory) {
		this(id, BoxType.OPERATOR, streamFactory);
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return state.getInputStream(INPUT_KEY);
	}

	@Override
	public Stream<OUT> getOutputStream(String reqId) {
		return state.getOutputStream(OUTPUT_KEY, this);
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		state.setInput(INPUT_KEY, in, this);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return state.getNext();
	}

	@Override
	public Collection<StreamProducer<?>> getPrevious() {
		return state.getPrevious();
	}

	@Override
	public void registerOut(StreamConsumer<OUT> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@Override
	public void run() {
		if (state.isEnabled()) {
			process();
		}
	}

	@Override
	public void activate() {
		state.enable();
	}

	@Override
	public void deActivate() {
		state.disable();
	}

	public boolean isActive() {
		return state.isEnabled();
	}

	public void process() {
		IN inTuple = getInputStream(getId()).getNextTuple();
		if (inTuple != null) {
			List<OUT> outTuples = processTuple(inTuple);
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

	public abstract List<OUT> processTuple(IN tuple);

	@Override
	public String toString() {
		return String.format("OP-%s", getId());
	}
}
