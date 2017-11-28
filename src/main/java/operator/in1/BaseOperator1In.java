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

package operator.in1;

import java.util.List;

import common.BoxState.BoxType;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.AbstractOperator;
import stream.Stream;
import stream.StreamFactory;

public abstract class BaseOperator1In<IN extends Tuple, OUT extends Tuple> extends AbstractOperator<IN, OUT>
		implements Operator1In<IN, OUT> {

	private final String INPUT_KEY = "INPUT";
	private final String OUTPUT_KEY = "OUTPUT";

	protected BaseOperator1In(String id, BoxType type, StreamFactory streamFactory) {
		super(id, type, streamFactory);
	}

	public BaseOperator1In(String id, StreamFactory streamFactory) {
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
	public void addOutput(StreamConsumer<OUT> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@Override
	public final void process() {
		IN inTuple = getInputStream(getId()).getNextTuple();
		if (inTuple != null) {
			List<OUT> outTuples = processTupleIn1(inTuple);
			if (outTuples != null) {
				for (OUT t : outTuples)
					getOutputStream(getId()).addTuple(t);
			}
		}
	}

}
