package scheduling.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import common.ActiveRunnable;
import scheduling.Scheduler;
import scheduling.TaskPool;

public class ThreadPoolScheduler implements Scheduler {

	private final TaskPool<ActiveRunnable> availableTasks;
	private final List<PoolWorkerThread> workers = new ArrayList<>();
	private final int maxThreads;
	private int nTasks;
	private final long quantum;
	private final TimeUnit timeUnit;
	private String statsFolder;
	private String executionId;

	public ThreadPoolScheduler(int maxThreads, long quantum, TimeUnit unit, TaskPool<ActiveRunnable> availableTasks) {
		this.availableTasks = availableTasks;
		this.maxThreads = maxThreads;
		this.quantum = quantum;
		this.timeUnit = unit;
	}

	@Override
	public void addTasks(Collection<? extends ActiveRunnable> tasks) {
		for (ActiveRunnable task : tasks) {
			availableTasks.register(task);
		}
		nTasks += tasks.size();
	}

	@Override
	public void startTasks() {
		if (!isEnabled()) {
			throw new IllegalStateException();
		}
		int nThreads = Math.min(maxThreads, nTasks);
		System.out.format("*** [%s] Starting %d worker threads%n", getClass().getSimpleName(), nThreads);
		for (int i = 0; i < nThreads; i++) {
			PoolWorkerThread worker = statsFolder != null
					? new PoolWorkerThreadStatistic(availableTasks, quantum, timeUnit, statsFolder, executionId)
					: new PoolWorkerThread(availableTasks, quantum, timeUnit);
			workers.add(worker);
		}
		for (PoolWorkerThread workerThread : workers) {
			workerThread.enable();
			workerThread.start();
		}
		// TODO: Observer pattern to detect thread crashes
	}

	@Override
	public void stopTasks() {
		if (isEnabled()) {
			throw new IllegalStateException();
		}
		for (PoolWorkerThread workerThread : workers) {
			try {
				workerThread.disable();
				workerThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
		workers.clear();
	}

	@Override
	public void activateStatistics(String folder, String executionId) {
		this.statsFolder = folder;
		this.executionId = executionId;
	}

	@Override
	public void enable() {
		availableTasks.enable();

	}

	@Override
	public boolean isEnabled() {
		return availableTasks.isEnabled();
	}

	@Override
	public void disable() {
		availableTasks.disable();
	}

}
