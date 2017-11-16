package scheduling.impl;

import java.util.concurrent.TimeUnit;

import operator.Operator;
import scheduling.ActiveThread;
import scheduling.TaskPool;

public class WorkerThread extends ActiveThread {
	private final TaskPool<Operator<?, ?>> availableTasks;
	private long interval;
	private final TimeUnit unit;

	private long runs;
	private long totalSchedulingCost;

	public WorkerThread(TaskPool<Operator<?, ?>> availableTasks, long interval, TimeUnit unit) {
		this.availableTasks = availableTasks;
		this.interval = interval;
		this.unit = unit;
	}

	@Override
	public void doRun() {
		runs++;
		long schedulingStart = System.nanoTime();
		Operator<?, ?> task = availableTasks.getNext(getId());
		// System.out.format("+ [T%d] %s%n", getId(), task);
		long runUntil = System.nanoTime() + unit.toNanos(interval);
		while (System.nanoTime() < runUntil && task.hasInput()) {
			task.run();
		}
		// System.out.format("- [T%d] %s%n", getId(), task);
		availableTasks.put(task);
		long schedulingEnd = System.nanoTime();
		totalSchedulingCost += schedulingEnd - schedulingStart - unit.toNanos(interval);
	}

	@Override
	public void disable() {
		System.out.println("Thread " + getId() + " average scheduling cost = "
				+ TimeUnit.NANOSECONDS.toMillis(totalSchedulingCost / runs) + "ms");
		super.disable();
	}

}
