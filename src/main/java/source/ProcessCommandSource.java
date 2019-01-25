/*
 * Copyright (C) 2017-2019
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

package source;

import common.component.ProcessCommand;
import common.tuple.Tuple;
import stream.Stream;

class ProcessCommandSource<T extends Tuple> implements ProcessCommand {
	private final Source<T> source;

	public ProcessCommandSource(Source<T> source) {
		this.source = source;
	}

	@Override
	public void run() {
		if (source.isEnabled()) {
			process();
		}
	}

	@Override
	public final void process() {
		T tuple = source.getNextTuple();
		Stream<T> output = source.getOutput();
		if (tuple != null) {
			output.addTuple(tuple);
		}
	}

}
