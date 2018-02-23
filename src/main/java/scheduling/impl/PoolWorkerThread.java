package scheduling.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import scheduling.ActiveThread;
import scheduling.TaskPool;

public class PoolWorkerThread extends ActiveThread {
	private final TaskPool<ActiveRunnable> availableTasks;
	private long quantum;
	private final TimeUnit unit;
	protected volatile boolean executed;

	private static AtomicInteger threadCount = new AtomicInteger();
	protected final int index;

	public PoolWorkerThread(TaskPool<ActiveRunnable> availableTasks, long quantum, TimeUnit unit) {
		this.availableTasks = availableTasks;
		this.quantum = quantum;
		this.unit = unit;
		this.index = threadCount.getAndIncrement();
	}

	@Override
	public void doRun() {
		ActiveRunnable task = getTask();
		if (task == null) {
			System.err.format("[WARN] %s was not given a task to execute. Ignoring...%n", this);
			return;
		}
		executeTask(task);
		putTask(task);
	}

	protected ActiveRunnable getTask() {
		return availableTasks.getNext(index);
	}

	protected void executeTask(ActiveRunnable task) {
		executed = false;
		task.onScheduled();
		long runUntil = System.nanoTime() + unit.toNanos(quantum);
		while (System.nanoTime() < runUntil && hasInput(task) && hasOutput(task)) {
			task.run();
			executed = true;
		}
	}

	protected void putTask(ActiveRunnable task) {
		if (executed) {
			task.onRun();
		}
		availableTasks.put(task, index);
	}

	@Override
	public void enable() {
		super.enable();
	}

	@Override
	public void disable() {
		super.disable();
	}

	boolean hasInput(ActiveRunnable task) {
		return (task instanceof StreamConsumer == false) || ((StreamConsumer<?>) task).hasInput();
	}

	boolean hasOutput(ActiveRunnable task) {
		return (task instanceof StreamProducer == false) || ((StreamProducer<?>) task).hasOutput();
	}

}
