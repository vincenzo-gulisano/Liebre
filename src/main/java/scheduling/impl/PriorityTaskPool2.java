package scheduling.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import operator.Operator;
import operator.PriorityMetric;
import scheduling.TaskPool;

public class PriorityTaskPool2 implements TaskPool<Operator<?, ?>> {
	private final PriorityMetric metric;
	private final ConcurrentHashMap<String, Operator<?, ?>> available = new ConcurrentHashMap<>();
	private final AtomicReference<List<OperatorPriority>> priorities = new AtomicReference<>(new ArrayList<>());
	private final AtomicLong ctr = new AtomicLong(0);
	private volatile int nTasks = 0;
	private final long nThreads;

	public PriorityTaskPool2(PriorityMetric metric, int nThreads) {
		this.metric = metric;
		this.nThreads = nThreads;
	}

	@Override
	public void register(Operator<?, ?> task) {
		available.put(task.getId(), task);
		priorities.get().add(new OperatorPriority(task, metric));
		nTasks++;
	}

	@Override
	public Operator<?, ?> getNext(long threadId) {
		if (ctr.get() == threadId) {
			// System.out.println("THREAD " + threadId + " updating array");
			List<OperatorPriority> oldPriorities = priorities.get();
			List<OperatorPriority> newPriorities = new ArrayList<>(nTasks);
			for (OperatorPriority pair : oldPriorities) {
				newPriorities.add(new OperatorPriority(pair.getOperator(), metric));
			}
			Collections.sort(newPriorities);
			priorities.set(newPriorities);
			ctr.set((threadId + 1) % nThreads);
			// System.out.println("Set ctr to " + ((threadId + 1) % N_THREADS));
		}
		List<OperatorPriority> currentPriorities = priorities.get();
		if (currentPriorities.get(0).getPriority() <= 0) {
			while (true) {
				OperatorPriority pair = currentPriorities
						.get(ThreadLocalRandom.current().nextInt(currentPriorities.size()));
				if (available.remove(pair.getOperator().getId()) != null) {
					return pair.getOperator();
				}
			}
		}
		for (OperatorPriority pair : currentPriorities) {
			// Get first available operator
			if (available.remove(pair.getOperator().getId()) != null) {
				return pair.getOperator();
			}
		}
		throw new IllegalStateException("No operator available for execution!");
	}

	@Override
	public void put(Operator<?, ?> task) {
		available.put(task.getId(), task);
	}

	@Override
	public void enable() {
	}

	@Override
	public boolean isEnabled() {
		return true;
	}

	@Override
	public void disable() {
	}

}
