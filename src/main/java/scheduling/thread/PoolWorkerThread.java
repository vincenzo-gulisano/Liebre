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

import common.component.Component;
import java.util.concurrent.TimeUnit;
import scheduling.TaskPool;

public class PoolWorkerThread extends LiebreThread {
	private final TaskPool<Component> taskPool;
	private long quantumNanos;
	protected volatile boolean executed;

	public PoolWorkerThread(int index, TaskPool<Component> availableTasks, long quantum, TimeUnit unit) {
		super(index);
		this.taskPool = availableTasks;
		this.quantumNanos = unit.toNanos(quantum);
	}

	@Override
	public void doRun() {
		Component task = getTask();
		if (task == null) {
			System.err.format("[WARN] %s was not given a task to execute. Ignoring...%n", this);
			return;
		}
		executeTask(task);
		putTask(task);
	}

	protected Component getTask() {
		return taskPool.getNext(getIndex());
	}

	protected void executeTask(Component task) {
		executed = false;
		task.onScheduled();
		final long runUntil = System.nanoTime() + quantumNanos;
		while (System.nanoTime() < runUntil && task.canRun()) {
			task.run();
			executed = true;
		}
	}

	protected void putTask(Component task) {
		if (executed) {
			task.onRun();
		}
		taskPool.put(task, getIndex());
	}

	@Override
	public void enable() {
		super.enable();
	}

	@Override
	public void disable() {
		super.disable();
	}


}
