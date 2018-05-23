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

import common.StreamConsumer;
import common.component.ComponentState;
import common.component.ComponentType;
import common.tuple.Tuple;
import java.util.Collection;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class BaseSource<OUT extends Tuple> implements Source<OUT> {

	private final ComponentState<Tuple, OUT> state;

	private final String OUTPUT_KEY = "OUTPUT";
	private final SourceFunction<OUT> function;
	private final ProcessCommandSource<OUT> processCommand = new ProcessCommandSource<>(this);

	public BaseSource(String id, SourceFunction<OUT> function) {
		state = new ComponentState<>(id, ComponentType.SOURCE, null);
		this.function = function;
	}

	@Override
	public void addOutput(StreamConsumer<OUT> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@Override
	public Stream<OUT> getOutputStream(String reqId) {
		return state.getOutputStream(OUTPUT_KEY, this);
	}

	@Override
	public boolean hasOutput() {
		return state.hasOutput();
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
	public Collection<StreamConsumer<OUT>> getNext() {
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
	public OUT getNextTuple() {
		return function.getNextTuple();
	}

	@Override
	public String getId() {
		return state.getId();
	}

	@Override
	public int getIndex() {
		return state.getIndex();
	}

	@Override
	public void onScheduled() {
	}

	@Override
	public void onRun() {
	}

	@Override
	public void setPriorityMetric(PriorityMetric metric) {
		processCommand.setMetric(metric);
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
