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

import java.util.Collection;

import common.BoxState;
import common.BoxState.BoxType;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

public class BaseSink<IN extends Tuple> implements Sink<IN> {

	protected final BoxState<IN, Tuple> state;
	protected final SinkFunction<IN> function;
	private final String INPUT_KEY = "INPUT";

	public BaseSink(String id, StreamFactory streamFactory, SinkFunction<IN> function) {
		state = new BoxState<>(id, BoxType.SINK, streamFactory);
		this.function = function;
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		state.setInput(INPUT_KEY, in, this);
	}

	@Override
	public Stream<IN> getInputStream(String reqId) {
		return state.getInputStream(INPUT_KEY);
	}

	@Override
	public void run() {
		while (state.isEnabled()) {
			process();
		}
	}

	@Override
	public void enable() {
		state.enable();
		function.enable();
	}

	@Override
	public void disable() {
		state.disable();
		function.disable();
	}

	@Override
	public boolean isEnabled() {
		return state.isEnabled();
	}

	public void process() {
		IN t = getInputStream(getId()).getNextTuple();
		if (t != null) {
			processTuple(t);
		}
	}

	@Override
	public String getId() {
		return state.getId();
	}

	@Override
	public Collection<StreamProducer<? extends Tuple>> getPrevious() {
		return state.getPrevious();
	}

	public void processTuple(IN tuple) {
		function.processTuple(tuple);
	}

	public BoxState<IN, Tuple> getState() {
		return state;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((state == null) ? 0 : state.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof BaseSink))
			return false;
		BaseSink<?> other = (BaseSink<?>) obj;
		if (state == null) {
			if (other.state != null)
				return false;
		} else if (!state.equals(other.state))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return getId();
	}

}
