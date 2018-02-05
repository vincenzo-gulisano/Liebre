package scheduling.impl;

import java.util.concurrent.TimeUnit;

import operator.Operator;
import scheduling.ActiveThread;
import scheduling.TaskPool;

public class PoolWorkerThread extends ActiveThread {
	private final TaskPool<Operator<?, ?>> availableTasks;
	private long quantum;
	private final TimeUnit unit;

	private static long threadCount = 0;
	protected final long index;

	public PoolWorkerThread(TaskPool<Operator<?, ?>> availableTasks, long quantum, TimeUnit unit) {
		this.availableTasks = availableTasks;
		this.quantum = quantum;
		this.unit = unit;
		this.index = threadCount;
		threadCount++;
	}

	@Override
	public void doRun() {
		Operator<?, ?> task = getTask();
		if (task == null) {
			System.err.format("[WARN] %s was not given an operator to execute. Ignoring...%n", this);
			return;
		}
		executeTask(task);
		putTask(task);
	}

	protected Operator<?, ?> getTask() {
		return availableTasks.getNext(index);
	}

	protected boolean executeTask(Operator<?, ?> task) {
		boolean executed = false;
		long runUntil = System.nanoTime() + unit.toNanos(quantum);
		while (System.nanoTime() < runUntil && task.hasInput() && task.hasOutput()) {
			task.run();
			executed = true;
		}
		return executed;
	}

	protected void putTask(Operator<?, ?> task) {
		availableTasks.put(task);
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
