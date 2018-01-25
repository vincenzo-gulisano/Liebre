package scheduling.impl;

import java.util.concurrent.TimeUnit;

import operator.Operator;
import scheduling.ActiveThread;
import scheduling.TaskPool;

public class PoolWorkerThread extends ActiveThread {
	private final TaskPool<Operator<?, ?>> availableTasks;
	private long quantum;
	private final TimeUnit unit;
	private long runs = 0;
	private long time = 0;

	private static long threadCount = 0;
	private final long index;

	public PoolWorkerThread(TaskPool<Operator<?, ?>> availableTasks, long quantum, TimeUnit unit) {
		this.availableTasks = availableTasks;
		this.quantum = quantum;
		this.unit = unit;
		this.index = threadCount;
		threadCount++;
	}

	@Override
	public void doRun() {
		long start = System.nanoTime();
		long schedulingTime = 0;
		Operator<?, ?> task = availableTasks.getNext(index);
		schedulingTime += System.nanoTime() - start;
		runs++;
		// System.out.format("+ [T%d] %s%n", getId(), task);
		long runUntil = System.nanoTime() + unit.toNanos(quantum);
		while (System.nanoTime() < runUntil && task.hasInput() && task.hasOutput()) {
			task.run();
		}
		start = System.nanoTime();
		// System.out.format("- [T%d] %s%n", getId(), task);
		availableTasks.put(task);
		schedulingTime += System.nanoTime() - start;
		time += schedulingTime;
	}

	@Override
	public void disable() {
		super.disable();
		System.out.format("T%d Scheduling Average = %3.2f ns%n", getId(), time / (double) runs);
	}

}
