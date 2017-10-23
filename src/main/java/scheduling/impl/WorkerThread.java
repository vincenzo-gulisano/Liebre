package scheduling.impl;

import java.util.concurrent.TimeUnit;

import scheduling.ActiveThread;
import scheduling.TaskPool;

public class WorkerThread extends ActiveThread {
	private final TaskPool<Runnable> availableTasks;
	private long interval;
	private final TimeUnit unit;

	public WorkerThread(TaskPool<Runnable> availableTasks, long interval, TimeUnit unit) {
		this.availableTasks = availableTasks;
		this.interval = interval;
		this.unit = unit;
	}

	@Override
	public void doRun() {
		Runnable task = availableTasks.getNext(getId());
		long runUntil = System.nanoTime() + unit.toNanos(interval);
		while (System.nanoTime() < runUntil) {
			task.run();
		}
		availableTasks.put(task);
	}

}
