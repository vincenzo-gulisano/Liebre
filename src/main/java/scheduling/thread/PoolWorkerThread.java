package scheduling.thread;

import java.util.concurrent.TimeUnit;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import scheduling.TaskPool;

public class PoolWorkerThread extends ActiveThread {
	private final TaskPool<ActiveRunnable> taskPool;
	private long quantumNanos;
	protected volatile boolean executed;

	protected final int index;

	public PoolWorkerThread(int index, TaskPool<ActiveRunnable> availableTasks, long quantum, TimeUnit unit) {
		this.index = index;
		this.taskPool = availableTasks;
		this.quantumNanos = unit.toNanos(quantum);
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
		return taskPool.getNext(index);
	}

	protected void executeTask(ActiveRunnable task) {
		executed = false;
		task.onScheduled();
		final long runUntil = System.nanoTime() + quantumNanos;
		while (System.nanoTime() < runUntil && hasInput(task) && hasOutput(task)) {
			task.run();
			executed = true;
		}
	}

	protected void putTask(ActiveRunnable task) {
		if (executed) {
			task.onRun();
		}
		taskPool.update(task, index);
		taskPool.put(task, index);
	}

	@Override
	public void enable() {
		super.enable();
	}

	@Override
	public void disable() {
		super.disable();
	}

	private boolean hasInput(ActiveRunnable task) {
		return (task instanceof StreamConsumer == false) || ((StreamConsumer<?>) task).hasInput();
	}

	private boolean hasOutput(ActiveRunnable task) {
		return (task instanceof StreamProducer == false) || ((StreamProducer<?>) task).hasOutput();
	}

}
