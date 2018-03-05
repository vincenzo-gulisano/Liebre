package scheduling.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import common.ActiveRunnable;
import common.util.AliasMethod;
import common.util.StatisticFilename;
import scheduling.TaskPool;
import scheduling.priority.PriorityMetric;
import scheduling.priority.PriorityMetricFactory;

public class ProbabilisticTaskPool implements TaskPool<ActiveRunnable> {

	private static class Turn {
		private final long ts;
		private final long threadId;
		private final long turnPeriodNanos;

		public Turn(long threadId, long turnPeriodNanos) {
			this.ts = System.nanoTime();
			this.threadId = threadId;
			this.turnPeriodNanos = turnPeriodNanos;
		}

		public Turn next(int nThreads) {
			return new Turn((threadId + 1) % nThreads, turnPeriodNanos);
		}

		public boolean isTime(long threadId) {
			return this.threadId == threadId && (System.nanoTime() >= ts + turnPeriodNanos);
		}
	}

	// FIXME: is tasks threadSafe??
	protected final List<ActiveRunnable> tasks = new ArrayList<>();
	protected final List<ActiveRunnable> passiveTasks = new ArrayList<>();
	private AtomicReference<AliasMethod> sampler = new AtomicReference<AliasMethod>(null);
	private final PriorityMetricFactory metricFactory;
	private volatile PriorityMetric metric;
	private volatile int nThreads;
	private final AtomicReference<Turn> turns;
	private AtomicReferenceArray<Boolean> available;
	private volatile boolean enabled;

	private final int priorityScalingFactor;

	private final CSVPrinter csv;
	private final boolean statisticsEnabled;

	// FIXME: Builder
	public ProbabilisticTaskPool(PriorityMetricFactory metricFactory, int priorityScalingFactor,
			long priorityUpdateInterval) {
		this(metricFactory, priorityScalingFactor, priorityUpdateInterval, null);
	}

	public ProbabilisticTaskPool(PriorityMetricFactory metricFactory, int priorityScalingFactor,
			long priorityUpdateInterval, String statisticsFolder) {
		this.metricFactory = metricFactory;
		this.priorityScalingFactor = priorityScalingFactor;
		this.turns = new AtomicReference<Turn>(new Turn(0, priorityUpdateInterval));
		// TODO: Refactor/remove
		if (statisticsFolder != null) {
			try {
				csv = new CSVPrinter(
						new FileWriter(StatisticFilename.INSTANCE.get(statisticsFolder, "taskPool", "prio")),
						CSVFormat.DEFAULT);
				this.statisticsEnabled = true;
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		} else {
			this.csv = null;
			this.statisticsEnabled = false;
		}
	}

	@Override
	public void register(ActiveRunnable task) {
		if (isEnabled()) {
			throw new IllegalStateException("Cannot add tasks in an enabled TaskPool!");
		}
		tasks.add(task);
	}

	@Override
	public void registerPassive(ActiveRunnable task) {
		if (isEnabled()) {
			throw new IllegalStateException("Cannot add tasks in an enabled TaskPool!");
		}
		tasks.add(task);
		passiveTasks.add(task);
	}

	@Override
	public ActiveRunnable getNext(int threadId) {
		Turn turn = turns.get();
		if (turn.isTime(threadId)) {
			updatePriorities(threadId);
			turns.set(turn.next(nThreads));
		}
		AliasMethod alias = sampler.get();
		while (true) {
			int k = alias.next();
			if (available.compareAndSet(k, true, false)) {
				return tasks.get(k);
			}
		}
	}

	@Override
	public void put(ActiveRunnable task, int threadId) {
		available.set(task.getIndex(), true);
	}

	private void updatePriorities(long threadId) {
		List<Double> probabilities = metric.getPriorities(priorityScalingFactor);
		recordStatistics(probabilities, threadId);
		sampler.set(new AliasMethod(probabilities));
	}

	@Override
	public void setThreadsNumber(int activeThreads) {
		if (isEnabled()) {
			throw new IllegalStateException("Cannot set threads number when TaskPool is enabled");
		}
		this.nThreads = activeThreads;
	}

	@Override
	public void enable() {

		if (nThreads == 0) {
			throw new IllegalStateException("Thread number not set!");
		}
		// Initialize locks and operator index
		available = new AtomicReferenceArray<>(tasks.size());
		// Sort tasks according to their indexes
		tasks.sort((ActiveRunnable t1, ActiveRunnable t2) -> Integer.compare(t1.getIndex(), t2.getIndex()));
		metric = metricFactory.newInstance(tasks, passiveTasks, nThreadsTotal());
		for (ActiveRunnable task : tasks) {
			boolean isActive = !passiveTasks.contains(task);
			// Only the active tasks can be picked for execution
			// by the task pool
			available.set(task.getIndex(), isActive);
			task.setPriorityMetric(metric);
			task.enable();
		}
		recordStatisticsHeader();
		// Initialize priorities
		updatePriorities(1);
		this.enabled = true;
	}

	@Override
	public boolean isEnabled() {
		return this.enabled;
	}

	@Override
	public void disable() {
		this.enabled = false;
		for (ActiveRunnable t : tasks) {
			t.disable();
		}
		if (statisticsEnabled) {
			try {
				csv.close();
			} catch (IOException e) {
				System.err.format("[WARN] Failed to close statistics file for TaskPool: %s%n", e.getMessage());
			}
		}
	}

	private int nThreadsTotal() {
		return nThreads + passiveTasks.size();
	}

	private void recordStatisticsHeader() {
		if (statisticsEnabled) {
			try {
				csv.printRecord(tasks);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private void recordStatistics(List<Double> probabilities, long threadId) {
		if (!isEnabled()) {
			System.err.println("[WARN] Ignoring append, TaskPool is disabled");
			return;
		}
		if (statisticsEnabled && threadId % 4 == 0) {
			try {
				csv.printRecord(probabilities);
			} catch (IOException e) {
				System.err.format("[WARN] Failed to record statistics for TaskPool: %s%n", e.getMessage());
			}
		}
	}

}
