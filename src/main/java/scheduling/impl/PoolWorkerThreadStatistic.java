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
	private final CountStatistic timesScheduledStatistic;
	private final CountStatistic timesRunStatistic;

	public PoolWorkerThreadStatistic(TaskPool<Operator<?, ?>> availableTasks, long quantum, TimeUnit unit,
			String statsFolder, String executionId) {
		super(availableTasks, quantum, unit);
		schedulingTimeStatistic = new AverageStatistic(
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
		sb.append(index);
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
	protected Operator<?, ?> getTask() {
		long start = System.nanoTime();
		Operator<?, ?> task = super.getTask();
		schedulingTimeStatistic.append(System.nanoTime() - start);
		timesScheduledStatistic.append(1L);
		return task;
	}

	@Override
	protected boolean executeTask(Operator<?, ?> task) {
		long start = System.nanoTime();
		boolean executed = super.executeTask(task);
		actualQuantumStatistic.append(System.nanoTime() - start);
		if (executed) {
			timesRunStatistic.append(1L);
		}
		return executed;
	}

	@Override
	protected void putTask(Operator<?, ?> task) {
		long start = System.nanoTime();
		super.putTask(task);
		schedulingTimeStatistic.append(System.nanoTime() - start);
	}

}