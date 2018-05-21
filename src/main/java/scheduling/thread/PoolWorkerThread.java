package scheduling.thread;

import java.util.concurrent.TimeUnit;

import common.component.Component;
import common.StreamConsumer;
import common.StreamProducer;
import scheduling.TaskPool;

public class PoolWorkerThread extends LiebreThread {
	private final TaskPool<Component> taskPool;
	private long quantumNanos;
	protected volatile boolean executed;

	public PoolWorkerThread(int index, TaskPool<Component> availableTasks, long quantum, TimeUnit unit) {
		super(index);
		this.taskPool = availableTasks;
		this.quantumNanos = unit.toNanos(quantum);
	}

	@Override
	public void doRun() {
		Component task = getTask();
		if (task == null) {
			System.err.format("[WARN] %s was not given a task to execute. Ignoring...%n", this);
			return;
		}
		executeTask(task);
		putTask(task);
	}

	protected Component getTask() {
		return taskPool.getNext(getIndex());
	}

	protected void executeTask(Component task) {
		executed = false;
		task.onScheduled();
		final long runUntil = System.nanoTime() + quantumNanos;
		while (System.nanoTime() < runUntil && hasInput(task) && hasOutput(task)) {
			task.run();
			executed = true;
		}
	}

	protected void putTask(Component task) {
		if (executed) {
			task.onRun();
		}
		taskPool.put(task, getIndex());
	}

	@Override
	public void enable() {
		super.enable();
	}

	@Override
	public void disable() {
		super.disable();
	}

	private boolean hasInput(Component task) {
		return (task instanceof StreamConsumer == false) || ((StreamConsumer<?>) task).hasInput();
	}

	private boolean hasOutput(Component task) {
		return (task instanceof StreamProducer == false) || ((StreamProducer<?>) task).hasOutput();
	}

}
