/*
 * Copyright (C) 2017-2018
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package sink;

import common.component.ProcessCommand;
import common.tuple.Tuple;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class ProcessCommandSink<T extends Tuple> implements ProcessCommand {

	private final Sink<T> sink;
	private PriorityMetric metric = PriorityMetric.noopMetric();

	public ProcessCommandSink(Sink<T> sink) {
		this.sink = sink;
	}

	@Override
	public void run() {
		if (sink.isEnabled()) {
			process();
		}
	}

	@Override
	public final void process() {
		Stream<T> input = sink.getInput();
		T tuple = input.getNextTuple();
		if (tuple != null) {
			metric.recordTupleRead(tuple, input);
			sink.processTuple(tuple);
		}
	}

	public void setMetric(PriorityMetric metric) {
		this.metric = metric;
	}

}
