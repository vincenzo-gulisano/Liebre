package scheduling.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import common.ActiveRunnable;
import scheduling.Scheduler;
import scheduling.thread.BasicWorkerThread;

/**
 * Scheduler implementation in case no scheduling is actually needed and the
 * requirement is just one thread per operator.
 * 
 * @author palivosd
 *
 */
public class NoopScheduler implements Scheduler {
	private final List<ActiveRunnable> tasks = new ArrayList<>();
	private final List<BasicWorkerThread> threads = new ArrayList<>();
	private volatile boolean enabled;

	@Override
	public void addTasks(Collection<? extends ActiveRunnable> tasks) {
		this.tasks.addAll(tasks);
	}

	@Override
	public void startTasks() {
		if (!isEnabled()) {
			throw new IllegalStateException();
		}
		for (Runnable operator : tasks) {
			BasicWorkerThread thread = new BasicWorkerThread(operator);
			threads.add(thread);
			thread.enable();
			thread.start();
		}
	}

	@Override
	public void stopTasks() {
		if (isEnabled()) {
			throw new IllegalStateException();
		}
		for (BasicWorkerThread thread : threads) {
			try {
				thread.disable();
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void enable() {
		for (ActiveRunnable task : tasks) {
			task.enable();
		}
		this.enabled = true;
	}

	@Override
	public boolean isEnabled() {
		return this.enabled;
	}

	@Override
	public void disable() {
		this.enabled = false;
		for (ActiveRunnable task : tasks) {
			task.disable();
		}
	}

	@Override
	public void activateStatistics(String folder, String executionId) {
		System.out.format("*** [%s] No statistics available%n", getClass().getSimpleName());
	}

}
