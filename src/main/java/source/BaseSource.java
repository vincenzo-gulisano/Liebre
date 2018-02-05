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

	private final BoxState<Tuple, T> state;
	private final String OUTPUT_KEY = "OUTPUT";
	private final SourceFunction<T> function;
	private final ProcessCommandSource<T> processCommand = new ProcessCommandSource<>(this);

	public BaseSource(String id, SourceFunction<T> function) {
		state = new BoxState<>(id, BoxType.SOURCE, null);
		this.function = function;
	}

	@Override
	public void addOutput(StreamConsumer<T> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@Override
	public Stream<T> getOutputStream(String reqId) {
		return state.getOutputStream(OUTPUT_KEY, this);
	}

	@Override
	public void run() {
		processCommand.run();
	}

	@Override
	public void enable() {
		state.enable();
		function.enable();
	}

	@Override
	public Collection<StreamConsumer<T>> getNext() {
		return state.getNext();
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

	@Override
	public T getNextTuple() {
		return function.getNextTuple();
	}

	@Override
	public String getId() {
		return state.getId();
	}

	@Override
	public void onScheduled() {
	}

	@Override
	public void onRun() {
	}

	@Override
	public String toString() {
		return getId();
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
		if (!(obj instanceof BaseSource))
			return false;
		BaseSource<?> other = (BaseSource<?>) obj;
		if (state == null) {
			if (other.state != null)
				return false;
		} else if (!state.equals(other.state))
			return false;
		return true;
	}

}
