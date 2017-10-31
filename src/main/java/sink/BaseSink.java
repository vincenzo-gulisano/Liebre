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

package sink;

import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

public class BaseSink<IN extends Tuple> implements Sink<IN> {

	protected Stream<IN> input;
	protected boolean active = false;
	protected final String id;
	protected final StreamFactory streamFactory;
	protected final SinkFunction<IN> function;

	public BaseSink(String id, StreamFactory streamFactory, SinkFunction<IN> function) {
		this.id = id;
		this.streamFactory = streamFactory;
		this.function = function;
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		if (active) {
			throw new IllegalStateException();
		}
		if (!in.getNext().contains(this)) {
			throw new UnsupportedOperationException("Please use registerOut() to construct query graphs");
		}
		this.input = streamFactory.newStream(in.getId(), id);
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return input;
	}

	@Override
	public void run() {
		while (active) {
			process();
		}
	}

	@Override
	public void activate() {
		if (input == null) {
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
		IN t = getInputStream(id).getNextTuple();
		if (t != null) {
			processTuple(t);
		}
	}

	@Override
	public String getId() {
		return this.id;
	}

	public void processTuple(IN tuple) {
		function.processTuple(tuple);
	}
}
