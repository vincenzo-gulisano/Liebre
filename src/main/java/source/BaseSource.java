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

package source;

import java.util.Collection;

import common.BoxState;
import common.BoxState.BoxType;
import common.StreamConsumer;
import common.tuple.Tuple;
import stream.Stream;

public class BaseSource<T extends Tuple> implements Source<T> {

	protected final BoxState<Tuple, T> state;
	private final String OUTPUT_KEY = "OUTPUT";
	protected final SourceFunction<T> function;

	public BaseSource(String id, SourceFunction function) {
		state = new BoxState<>(id, BoxType.SOURCE, null);
		this.function = function;
	}

	@Override
	public void registerOut(StreamConsumer<T> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@Override
	public Stream<T> getOutputStream(String reqId) {
		return state.getOutputStream(OUTPUT_KEY, this);
	}

	@Override
	public void run() {
		while (state.isEnabled()) {
			process();
		}
	}

	@Override
	public void activate() {
		state.enable();
	}

	@Override
	public Collection<StreamConsumer<T>> getNext() {
		return state.getNext();
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
		T t = function.getNextTuple();
		if (t != null)
			getOutputStream(getId()).addTuple(t);
	}

	@Override
	public String getId() {
		return state.getId();
	}
}
