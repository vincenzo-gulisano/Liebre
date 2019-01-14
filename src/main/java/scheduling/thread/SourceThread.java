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

package scheduling.thread;

import java.util.concurrent.TimeUnit;

import common.component.Component;
import source.Source;

public class SourceThread extends LiebreThread {

	private final Component source;
	private final long quantumNanos;

	public SourceThread(int index, Component source, long quantum, TimeUnit unit) {
		super(index);
		if (source instanceof Source<?> == false) {
			throw new IllegalArgumentException(
					String.format("%s only accept tasks of type Source", getClass().getSimpleName()));
		}
		this.source = source;
		this.quantumNanos = unit.toNanos(quantum);
	}

	@Override
	protected void doRun() {
		source.onScheduled();
		final long runUntil = System.nanoTime() + quantumNanos;
		while (System.nanoTime() < runUntil && source.canRun()) {
			source.run();
		}
		source.onRun();
	}

}
