package scheduling.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import common.util.AliasMethod;
import common.util.StatisticFilename;
import operator.Operator;
import operator.PriorityMetric;
import scheduling.TaskPool;

public class ProbabilisticTaskPool implements TaskPool<Operator<?, ?>> {

	protected final List<Operator<?, ?>> operators = new ArrayList<>();
	private AtomicReference<AliasMethod> sampler = new AtomicReference<AliasMethod>(null);
	private final PriorityMetric metric;
	private final int nThreads;
	private final AtomicLong ctr = new AtomicLong(0);
	private ConcurrentHashMap<String, Boolean> available = new ConcurrentHashMap<>();
	private boolean enabled;

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
		if (statisticsFolder != null) {
			// TODO: Refactor/remove
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
	public void enable() {
		// TODO: remove/refactor
		if (statisticsEnabled) {
			try {
				csv.printRecord(operators);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
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
		if (statisticsEnabled) {
			try {
				csv.close();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	@Override
	public void register(Operator<?, ?> task) {
		if (isEnabled()) {
			throw new IllegalStateException("Cannot add operators in an enabled TaskPool!");
		}
		operators.add(task);
		available.put(task.getId(), true);
	}

	@Override
	public Operator<?, ?> getNext(long threadId) {
		if (ctr.get() == threadId) {
			updatePriorities(threadId);
			ctr.set((threadId + 1) % nThreads);
		}
		AliasMethod alias = sampler.get();
		while (true) {
			int k = alias.next();
			Operator<?, ?> chosenOperator = operators.get(k);
			if (available.replace(chosenOperator.getId(), true, false)) {
				return chosenOperator;
			}
		}
	}

	@Override
	public void put(Operator<?, ?> task) {
		available.put(task.getId(), true);

	}

	private void updatePriorities(long threadId) {
		// FIXME: Shared array, do not create it each time
		List<Double> probabilities = new ArrayList<>();
		double prioritySum = 0;
		for (Operator<?, ?> operator : operators) {
			// Scale priority to emphasize the difference between high and low priorities
			double priority = Math.pow(metric.getPriority(operator), priorityScalingFactor);
			probabilities.add(priority);
			prioritySum += priority;
		}
		for (int i = 0; i < probabilities.size(); i++) {
			probabilities.set(i, probabilities.get(i) / prioritySum);
		}
		// TODO: Remove/refactor
		if (statisticsEnabled && threadId % 4 == 0) {
			try {
				csv.printRecord(probabilities);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		// System.out.println(probabilities);
		sampler.set(new AliasMethod(probabilities));
	}

}
