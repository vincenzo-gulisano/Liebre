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

package scheduling.thread;

import common.Active;
import common.util.StopJvmUncaughtExceptionHandler;

/**
 * Thread that can be stopped on demand.
 * 
 * @author palivosd
 *
 */
public abstract class LiebreThread extends Thread implements Active {
	private final int index;

	public LiebreThread() {
		this(-1);
	}

	public LiebreThread(int index) {
		this.index = index;
		setDefaultUncaughtExceptionHandler(StopJvmUncaughtExceptionHandler.INSTANCE);
	}

	@Override
	public void run() {
		while (isEnabled()) {
			doRun();
		}
	}

	protected abstract void doRun();

	@Override
	public void enable() {
	}

	@Override
	public boolean isEnabled() {
		return !isInterrupted();
	}

	@Override
	public void disable() {
		interrupt();
	}

	public int getIndex() {
		return index;
	}

}
