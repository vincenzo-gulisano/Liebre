package scheduling.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import operator.Operator;
import scheduling.Scheduler;
import scheduling.TaskPool;

public class ThreadPoolScheduler implements Scheduler {

	private final TaskPool<Operator<?, ?>> availableTasks;
	private final List<PoolWorkerThread> workers = new ArrayList<>();
	private final int maxThreads;
	private int nTasks;
	private final long quantum;
	private final TimeUnit timeUnit;

	public ThreadPoolScheduler(int maxThreads, long quantum, TimeUnit unit, TaskPool<Operator<?, ?>> availableTasks) {
		this.availableTasks = availableTasks;
		this.maxThreads = maxThreads;
		this.quantum = quantum;
		this.timeUnit = unit;
	}

	@Override
	public void addTasks(Collection<? extends Operator<?, ?>> tasks) {
		for (Operator<?, ?> task : tasks) {
			availableTasks.register(task);
		}
		nTasks += tasks.size();
	}

	@Override
	public void startTasks() {
		availableTasks.enable();
		int nThreads = Math.min(maxThreads, nTasks);
		System.out.format("[%s] Starting %d worker threads%n", getClass().getSimpleName(), nThreads);
		for (int i = 0; i < nThreads; i++) {
			workers.add(new PoolWorkerThread(availableTasks, quantum, timeUnit));
		}
		for (PoolWorkerThread workerThread : workers) {
			workerThread.enable();
			workerThread.start();
		}
		// TODO: Observer pattern to detect thread crashes
	}

	@Override
	public void stopTasks() {
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
		availableTasks.disable();
	}

}
