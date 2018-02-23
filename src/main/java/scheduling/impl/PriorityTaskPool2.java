package scheduling.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import common.ActiveRunnable;
import scheduling.TaskPool;
import scheduling.priority.PriorityMetric;
import source.Source;

@Deprecated
public class PriorityTaskPool2 implements TaskPool<ActiveRunnable> {
	private final PriorityMetric metric;
	private final ConcurrentHashMap<String, ActiveRunnable> available = new ConcurrentHashMap<>();
	private final AtomicReference<List<TaskPriority>> priorities = new AtomicReference<>(new ArrayList<>());
	private final AtomicLong ctr = new AtomicLong(0);
	private volatile int nTasks = 0;
	private final long nThreads;
	private volatile boolean enabled;

	public PriorityTaskPool2(PriorityMetric metric) {
		this.metric = metric;
		this.nThreads = 0;
		throw new IllegalStateException("TaskPool is deprecated");
	}

	@Override
	public void register(ActiveRunnable task) {
		if (task instanceof Source) {
			throw new IllegalStateException("This pool does not accept sources!");
		}
		available.put(task.getId(), task);
		priorities.get().add(new TaskPriority(task, metric));
		nTasks++;
	}

	@Override
	public ActiveRunnable getNext(int threadId) {
		if (ctr.get() == threadId) {
			// System.out.println("THREAD " + threadId + " updating array");
			List<TaskPriority> oldPriorities = priorities.get();
			List<TaskPriority> newPriorities = new ArrayList<>(nTasks);
			for (TaskPriority pair : oldPriorities) {
				newPriorities.add(new TaskPriority(pair.getTask(), metric));
			}
			Collections.sort(newPriorities);
			priorities.set(newPriorities);
			ctr.set((threadId + 1) % nThreads);
			// System.out.println("Set ctr to " + ((threadId + 1) % N_THREADS));
		}
		List<TaskPriority> currentPriorities = priorities.get();
		if (currentPriorities.get(0).getPriority() <= 0) {
			while (true) {
				TaskPriority pair = currentPriorities
						.get(ThreadLocalRandom.current().nextInt(currentPriorities.size()));
				if (available.remove(pair.getTask().getId()) != null) {
					return pair.getTask();
				}
			}
		}
		for (TaskPriority pair : currentPriorities) {
			// Get first available operator
			if (available.remove(pair.getTask().getId()) != null) {
				return pair.getTask();
			}
		}
		throw new IllegalStateException("No operator available for execution!");
	}

	@Override
	public void put(ActiveRunnable task, int threadId) {
		if (task instanceof Source) {
			throw new IllegalStateException("This pool does not accept sources!");
		}
		available.put(task.getId(), task);
	}

	@Override
	public void enable() {
		for (TaskPriority p : priorities.get()) {
			p.getTask().enable();
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
		for (TaskPriority p : priorities.get()) {
			p.getTask().disable();
		}
	}

	@Override
	public void update(ActiveRunnable task, int threadId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setThreadsNumber(int activeThreads) {
		throw new UnsupportedOperationException();
	}

}
