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

package scheduling.impl;

import java.util.concurrent.TimeUnit;

import common.component.Component;
import common.statistic.AverageStatistic;
import common.statistic.CountStatistic;
import common.util.StatisticFilename;
import scheduling.TaskPool;
import scheduling.thread.PoolWorkerThread;

//TODO: Decorate instead
public class PoolWorkerThreadStatistic extends PoolWorkerThread {
	private final CountStatistic schedulingTimeStatistic;
	private final AverageStatistic actualQuantumStatistic;
	private final CountStatistic timesScheduledStatistic;
	private final CountStatistic timesRunStatistic;

	public PoolWorkerThreadStatistic(int index, TaskPool<Component> availableTasks, long quantum, TimeUnit unit,
			String statsFolder, String executionId) {
		super(index, availableTasks, quantum, unit);
		schedulingTimeStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "schedtime"), true);
		timesScheduledStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "sched"), true);
		timesRunStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "runs"), true);
		actualQuantumStatistic = new AverageStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "quantum"), true);
	}

	private String threadId(String executionId) {
		StringBuilder sb = new StringBuilder("THREAD_");
		sb.append(getIndex());
		if (executionId != null && !executionId.isEmpty()) {
			sb.append(".").append(executionId);
		}
		return sb.toString();
	}

	@Override
	public void enable() {
		schedulingTimeStatistic.enable();
		timesScheduledStatistic.enable();
		timesRunStatistic.enable();
		actualQuantumStatistic.enable();
		super.enable();
	}

	@Override
	public void disable() {
		schedulingTimeStatistic.disable();
		timesScheduledStatistic.disable();
		timesRunStatistic.disable();
		actualQuantumStatistic.disable();
		super.disable();
	}

	@Override
	protected Component getTask() {
		long start = System.nanoTime();
		Component task = super.getTask();
		schedulingTimeStatistic.append(System.nanoTime() - start);
		return task;
	}

	@Override
	protected void executeTask(Component task) {
		// Measure scheduled statistic
		timesScheduledStatistic.append(1L);
		long start = System.nanoTime();
		super.executeTask(task);
		actualQuantumStatistic.append(System.nanoTime() - start);
	}

	@Override
	protected void putTask(Component task) {
		if (executed) {
			timesRunStatistic.append(1L);
		}
		long start = System.nanoTime();
		super.putTask(task);
		schedulingTimeStatistic.append(System.nanoTime() - start);
	}

}
