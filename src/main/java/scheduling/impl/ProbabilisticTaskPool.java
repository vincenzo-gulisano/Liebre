package scheduling.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import common.util.AliasMethod;
import operator.Operator;
import operator.PriorityMetric;
import scheduling.TaskPool;

public class ProbabilisticTaskPool implements TaskPool<Operator<?, ?>> {

	private final List<Operator<?, ?>> operators = new ArrayList<>();
	private AtomicReference<AliasMethod> sampler = new AtomicReference<AliasMethod>(null);
	private final PriorityMetric metric;
	private final int nThreads;
	private final AtomicLong ctr = new AtomicLong(0);
	private ConcurrentHashMap<String, Boolean> available = new ConcurrentHashMap<>();
	private boolean enabled;

	private final int priorityScalingFactor;

	public ProbabilisticTaskPool(PriorityMetric metric, int nThreads, int priorityScalingFactor) {
		this.metric = metric;
		this.nThreads = nThreads;
		this.priorityScalingFactor = priorityScalingFactor;
	}

	@Override
	public void enable() {
		updatePriorities();
		this.enabled = true;
	}

	@Override
	public boolean isEnabled() {
		return this.enabled;
	}

	@Override
	public void disable() {
		this.enabled = false;
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
			updatePriorities();
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

	private void updatePriorities() {
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
		// System.out.println(probabilities);
		sampler.set(new AliasMethod(probabilities));
	}

}
