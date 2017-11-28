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

package operator.Union;

import common.BoxState.BoxType;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.AbstractOperator;
import stream.Stream;
import stream.StreamFactory;

public class UnionOperator<IN extends Tuple> extends AbstractOperator<IN, IN> {
	private static final String OUTPUT_KEY = "OUTPUT";

	public UnionOperator(String id, StreamFactory streamFactory) {
		super(id, BoxType.UNION, streamFactory);
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		state.setInput(in.getId(), in, this);
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return state.getInputStream(reqId);
	}

	@Override
	public final void process() {
		for (Stream<IN> in : state.getInputs()) {
			IN inTuple = in.getNextTuple();
			if (inTuple != null) {
				getOutputStream(getId()).addTuple(inTuple);
			}
		}
	}

	@Override
	public void addOutput(StreamConsumer<IN> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@Override
	public Stream<IN> getOutputStream(String requestorId) {
		return state.getOutputStream(OUTPUT_KEY, this);
	}

}
