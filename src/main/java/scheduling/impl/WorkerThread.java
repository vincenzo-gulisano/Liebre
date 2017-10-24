package scheduling.impl;

import java.util.concurrent.TimeUnit;

import operator.Operator;
import scheduling.ActiveThread;
import scheduling.TaskPool;

public class WorkerThread extends ActiveThread {
	private final TaskPool<Operator<?, ?>> availableTasks;
	private long interval;
	private final TimeUnit unit;

	public WorkerThread(TaskPool<Operator<?, ?>> availableTasks, long interval, TimeUnit unit) {
		this.availableTasks = availableTasks;
		this.interval = interval;
		this.unit = unit;
	}

	@Override
	public void doRun() {
		Operator<?, ?> task = availableTasks.getNext(getId());
		// System.out.format("Thread %d executing %s%n", getId(), task.getId());
		System.out.format("+ [T%d] %s%n", getId(), task);
		long runUntil = System.nanoTime() + unit.toNanos(interval);
		while (System.nanoTime() < runUntil) {
			task.run();
		}
		System.out.format("- [T%d] %s%n", getId(), task);
		availableTasks.put(task);
	}

}
