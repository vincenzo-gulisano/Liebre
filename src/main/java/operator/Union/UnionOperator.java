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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import common.StreamProducer;
import common.tuple.Tuple;
import operator.BaseOperator;
import stream.Stream;
import stream.StreamFactory;

public class UnionOperator<IN extends Tuple> extends BaseOperator<IN, IN> {

	protected final Map<String, Stream<IN>> inputs = new ConcurrentHashMap<>();

	public UnionOperator(String id, StreamFactory streamFactory) {
		super(id, streamFactory);
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		Stream<IN> input = streamFactory.newStream(in.getId(), id);
		inputs.put(in.getId(), input);
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return inputs.get(reqId);
	}

	@Override
	public void process() {
		for (Stream<IN> in : inputs.values()) {
			IN inTuple = in.getNextTuple();
			if (inTuple != null) {
				getOutputStream(getId()).addTuple(inTuple);
			}
		}
	}

	@Override
	public void activate() {
		if (inputs.size() == 0 || next == null) {
			throw new IllegalStateException(id);
		}
		active = true;
	}

	@Override
	public List<IN> processTuple(IN tuple) {
		return null;
	}
}
