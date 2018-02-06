package scheduling.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import common.ActiveRunnable;
import common.util.AliasMethod;
import common.util.StatisticFilename;
import scheduling.TaskPool;
import scheduling.priority.PriorityMetric;

public class ProbabilisticTaskPool implements TaskPool<ActiveRunnable> {

	protected final List<ActiveRunnable> tasks = new ArrayList<>();
	private AtomicReference<AliasMethod> sampler = new AtomicReference<AliasMethod>(null);
	private final PriorityMetric metric;
	private final int nThreads;
	private final AtomicLong ctr = new AtomicLong(0);
	private volatile Map<String, Integer> taskIndex;
	private AtomicReferenceArray<Boolean> available;
	private volatile boolean enabled;

	private final int priorityScalingFactor;

	private final CSVPrinter csv;
	private final boolean statisticsEnabled;

	public ProbabilisticTaskPool(PriorityMetric metric, int nThreads, int priorityScalingFactor) {
		this(metric, nThreads, priorityScalingFactor, null);
	}

	public ProbabilisticTaskPool(PriorityMetric metric, int nThreads, int priorityScalingFactor,
			String statisticsFolder) {
		this.metric = metric;
		this.nThreads = nThreads;
		this.priorityScalingFactor = priorityScalingFactor;
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
			throw new IllegalStateException("Cannot add operators in an enabled TaskPool!");
		}
		tasks.add(task);
	}

	@Override
	public ActiveRunnable getNext(long threadId) {
		if (ctr.get() == threadId) {
			updatePriorities(threadId);
			ctr.set((threadId + 1) % nThreads);
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
	public void put(ActiveRunnable task) {
		available.set(taskIndex.get(task.getId()), true);

	}

	private void updatePriorities(long threadId) {
		// FIXME: Shared array, do not create it each time
		List<Double> probabilities = new ArrayList<>();
		double prioritySum = 0;
		int nanCount = 0;
		for (ActiveRunnable task : tasks) {
			double priority = getPriority(task);
			probabilities.add(priority);
			if (Double.isNaN(priority)) {
				nanCount++;
			} else {
				prioritySum += priority;
			}
		}
		// Set default priority for unknown metric values
		final double defaultProbability = prioritySum / (probabilities.size() - nanCount);
		prioritySum = 0;
		for (int i = 0; i < probabilities.size(); i++) {
			double priority = probabilities.get(i);
			if (Double.isNaN(priority)) {
				probabilities.set(i, defaultProbability);
			}
			prioritySum += probabilities.get(i);
		}
		// Final normalization
		for (int i = 0; i < probabilities.size(); i++) {
			probabilities.set(i, probabilities.get(i) / prioritySum);
		}
		recordStatistics(probabilities, threadId);
		// System.out.println(probabilities);
		sampler.set(new AliasMethod(probabilities));
	}

	private double getPriority(ActiveRunnable task) {
		double p = metric.getPriority(task);
		// Scale priority to emphasize the difference between high and low priorities
		return Double.isNaN(p) ? p : Math.pow(p, priorityScalingFactor);
	}

	@Override
	public void enable() {
		// TODO: remove/refactor
		if (statisticsEnabled) {
			try {
				csv.printRecord(tasks);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		// Initialize locks and operator index
		available = new AtomicReferenceArray<>(tasks.size());
		Map<String, Integer> tempIndex = new HashMap<>();
		for (int i = 0; i < tasks.size(); i++) {
			ActiveRunnable task = tasks.get(i);
			tempIndex.put(task.getId(), i);
			available.set(i, true);
			task.enable();
		}
		taskIndex = Collections.unmodifiableMap(tempIndex);
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
				throw new IllegalStateException(e);
			}
		}
	}

	private void recordStatistics(List<Double> probabilities, long threadId) {
		if (statisticsEnabled && threadId % 4 == 0) {
			try {
				csv.printRecord(probabilities);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}

}
