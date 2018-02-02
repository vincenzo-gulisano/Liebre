package scheduling.impl;

import java.util.concurrent.TimeUnit;

import common.statistic.AverageStatistic;
import common.statistic.CountStatistic;
import common.util.StatisticFilename;
import operator.Operator;
import scheduling.TaskPool;

public class PoolWorkerThreadStatistic extends PoolWorkerThread {
	private final AverageStatistic schedulingTimeStatistic;
	private final AverageStatistic actualQuantumStatistic;
	private final CountStatistic executionTimesStatistic;
	private final CountStatistic emptyExecutionsStatistic;

	public PoolWorkerThreadStatistic(TaskPool<Operator<?, ?>> availableTasks, long quantum, TimeUnit unit,
			String statsFolder, String executionId) {
		super(availableTasks, quantum, unit);
		schedulingTimeStatistic = new AverageStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "sched"), true);
		executionTimesStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "runs"), true);
		emptyExecutionsStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "noop"), true);
		actualQuantumStatistic = new AverageStatistic(
				StatisticFilename.INSTANCE.get(statsFolder, threadId(executionId), "quantum"), true);
	}

	private String threadId(String executionId) {
		StringBuilder sb = new StringBuilder("THREAD_");
		sb.append(index);
		if (executionId != null && !executionId.isEmpty()) {
			sb.append(".").append(executionId);
		}
		return sb.toString();
	}

	@Override
	public void enable() {
		schedulingTimeStatistic.enable();
		executionTimesStatistic.enable();
		emptyExecutionsStatistic.enable();
		actualQuantumStatistic.enable();
		super.enable();
	}

	@Override
	public void disable() {
		schedulingTimeStatistic.disable();
		executionTimesStatistic.disable();
		emptyExecutionsStatistic.disable();
		actualQuantumStatistic.disable();
		super.disable();
	}

	@Override
	protected Operator<?, ?> getTask() {
		long start = System.nanoTime();
		Operator<?, ?> task = super.getTask();
		schedulingTimeStatistic.append(System.nanoTime() - start);
		executionTimesStatistic.append(1L);
		if (task != null && (!task.hasInput() || !task.hasOutput()))
			emptyExecutionsStatistic.append(1L);
		return task;
	}

	@Override
	protected void executeTask(Operator<?, ?> task) {
		long start = System.nanoTime();
		super.executeTask(task);
		actualQuantumStatistic.append(System.nanoTime() - start);
	}

	@Override
	protected void putTask(Operator<?, ?> task) {
		long start = System.nanoTime();
		super.putTask(task);
		schedulingTimeStatistic.append(System.nanoTime() - start);
	}

}
