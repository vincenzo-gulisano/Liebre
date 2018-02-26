package scheduling.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import common.Active;
import common.ActiveRunnable;
import scheduling.Scheduler;
import scheduling.TaskPool;
import scheduling.thread.ActiveThread;
import scheduling.thread.PoolWorkerThread;
import scheduling.thread.SourceThread;
import source.Source;

public class ThreadPoolScheduler implements Scheduler {

	private final TaskPool<ActiveRunnable> taskPool;
	private final List<PoolWorkerThread> workers = new ArrayList<>();
	private final int maxThreads;
	private int nTasks;
	private final long quantum;
	private final TimeUnit timeUnit;
	private String statsFolder;
	private String executionId;
	private volatile boolean indepedentSources;
	private final List<SourceThread> sourceThreads = new ArrayList<>();
	private final List<Source<?>> sources = new ArrayList<>();
	private volatile int nThreads;

	public ThreadPoolScheduler(int maxThreads, long quantum, TimeUnit unit, TaskPool<ActiveRunnable> taskPool) {
		this.taskPool = taskPool;
		this.maxThreads = maxThreads;
		this.quantum = quantum;
		this.timeUnit = unit;
	}

	public ThreadPoolScheduler enableSourceThreads() {
		System.err.println("[WARN] Never call enableSourceThreads() after enable(). Bad things will happen!");
		this.indepedentSources = true;
		return this;
	}

	@Override
	public void addTasks(Collection<? extends ActiveRunnable> tasks) {
		for (ActiveRunnable task : tasks) {
			if (taskPool.schedulingMetricsEnabled()) {
				task.enableExecutionMetrics();
			}
			if (indepedentSources && task instanceof Source) {
				sources.add((Source<?>) task);
				taskPool.registerPassive(task);
			} else {
				taskPool.register(task);
				nTasks++;
			}
		}
	}

	@Override
	public void startTasks() {
		if (!isEnabled()) {
			throw new IllegalStateException();
		}
		System.out.format("*** [%s] Starting %d worker threads%n", getClass().getSimpleName(), nThreads);
		int threadIndex = 0;
		for (threadIndex = 0; threadIndex < nThreads; threadIndex++) {
			PoolWorkerThread worker = statsFolder != null
					? new PoolWorkerThreadStatistic(threadIndex, taskPool, quantum, timeUnit, statsFolder, executionId)
					: new PoolWorkerThread(threadIndex, taskPool, quantum, timeUnit);
			workers.add(worker);
			worker.enable();
			worker.start();
		}
		// Independent source threads
		System.out.format("*** [%s] Starting %d source threads%n", getClass().getSimpleName(), sources.size());
		for (ActiveRunnable task : sources) {
			SourceThread t = new SourceThread(threadIndex, task, taskPool, quantum, timeUnit);
			sourceThreads.add(t);
			t.enable();
			t.start();
			threadIndex++;
		}
	}

	@Override
	public void stopTasks() {
		if (isEnabled()) {
			throw new IllegalStateException();
		}
		for (ActiveThread workerThread : workers) {
			try {
				workerThread.disable();
				workerThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
		workers.clear();
		for (ActiveThread workerThread : sourceThreads) {
			try {
				workerThread.disable();
				workerThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
		sourceThreads.clear();
	}

	@Override
	public void activateStatistics(String folder, String executionId) {
		this.statsFolder = folder;
		this.executionId = executionId;
	}

	@Override
	public void enable() {
		nThreads = Math.min(maxThreads, nTasks);
		taskPool.setThreadsNumber(nThreads);
		taskPool.enable();
		for (Active s : sources) {
			s.enable();
		}
	}

	@Override
	public boolean isEnabled() {
		return taskPool.isEnabled();
	}

	@Override
	public void disable() {
		taskPool.disable();
		for (Active s : sources) {
			s.disable();
		}
	}

}
