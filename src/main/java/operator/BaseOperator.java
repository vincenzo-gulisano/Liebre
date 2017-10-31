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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

public abstract class BaseOperator<IN extends Tuple, OUT extends Tuple> implements Operator<IN, OUT> {

	protected Stream<IN> input;
	protected StreamConsumer<OUT> next;
	protected boolean active = false;
	protected final String id;
	protected final StreamFactory streamFactory;

	public BaseOperator(String id, StreamFactory streamFactory) {
		this.id = id;
		this.streamFactory = streamFactory;
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return input;
	}

	@Override
	public Stream<OUT> getOutputStream(String reqId) {
		return next.getInputStream(reqId);
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		if (active) {
			throw new IllegalStateException();
		}
		if (!in.getNext().contains(this)) {
			throw new UnsupportedOperationException("Please use registerOut() to construct query graphs");
		}
		input = streamFactory.newStream(in.getId(), id);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return Arrays.asList(this.next);
	}

	@Override
	public void registerOut(StreamConsumer<OUT> out) {
		this.next = out;
		out.registerIn(this);
	}

	@Override
	public void run() {
		if (active) {
			process();
		}
	}

	@Override
	public void activate() {
		if (input == null || next == null) {
			throw new IllegalStateException(id);
		}
		active = true;
	}

	@Override
	public void deActivate() {
		active = false;
	}

	public boolean isActive() {
		return active;
	}

	public void process() {
		IN inTuple = getInputStream(id).getNextTuple();
		if (inTuple != null) {
			List<OUT> outTuples = processTuple(inTuple);
			if (outTuples != null) {
				for (OUT t : outTuples)
					getOutputStream(id).addTuple(t);
			}
		}
	}

	@Override
	public synchronized long getPriority() {
		return getInputStream(getId()) != null ? getInputStream(getId()).size() : 0;
	}

	@Override
	public String getId() {
		return this.id;
	}

	public abstract List<OUT> processTuple(IN tuple);

	@Override
	public String toString() {
		return String.format("OP-%s [priority=%d]", id, getPriority());
	}
}
