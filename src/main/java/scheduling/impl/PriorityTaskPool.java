package scheduling.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

import common.StreamConsumer;
import common.StreamProducer;
import common.util.Util;
import operator.Operator;
import scheduling.TaskPool;
import scheduling.priority.PriorityMetric;
import scheduling.priority.QueueSizePriorityMetric;
import source.Source;

public class PriorityTaskPool implements TaskPool<Operator<?, ?>> {
	private final long firstUpdateInterval;
	private final PriorityMetric metric;
	private final PriorityBlockingQueue<TaskPriority> tasks;

	private final ConcurrentHashMap<String, LongAdder> calls = new ConcurrentHashMap<>();
	private final ExecutorService service;

	private final List<Operator<?, ?>> firstOperators = new ArrayList<>();
	private volatile boolean enabled;
	private final Runnable updateFirst = new Runnable() {

		@Override
		public void run() {
			for (Operator<?, ?> first : firstOperators) {
				updateOperator(first);
			}
			Util.sleep(firstUpdateInterval);
			if (isEnabled()) {
				service.submit(this);
			}
		}
	};

	public PriorityTaskPool(PriorityMetric metric, int numberHeperThreads, long firstUpdateInterval) {
		this.firstUpdateInterval = firstUpdateInterval;
		this.metric = metric;
		tasks = new PriorityBlockingQueue<>();
		service = Executors.newFixedThreadPool(numberHeperThreads);
	}

	@Override
	public void register(Operator<?, ?> task) {
		for (StreamProducer<?> previous : task.getPrevious()) {
			if (previous instanceof Source) {
				firstOperators.add(task);
				break;
			}
		}
		put(task);
	}

	@Override
	public Operator<?, ?> getNext(long threadId) {
		Operator<?, ?> task = takeTask();
		calls.computeIfAbsent(task.getId(), k -> new LongAdder()).increment();;
		return task;
	}

	@Override
	public void put(Operator<?, ?> task) {
		tasks.offer(new TaskPriority(task, metric));
		for (StreamConsumer<?> next : task.getNext()) {
			updateOperator(next);
		}
	}

	private Operator<?, ?> takeTask() {
		try {
			Operator<?, ?> task = tasks.take().getTask();
			return task;
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException();
		}
	}

	private void updateOperator(Object obj) {
		if (obj instanceof Operator == false) {
			return;
		}
		Operator<?, ?> op = (Operator<?, ?>) obj;
		if (tasks.remove(TaskPriority.empty(op))) {
			tasks.offer(new TaskPriority(op, metric));
		}
	}

	@Override
	public void enable() {
		enabled = true;
		service.submit(updateFirst);
	}

	@Override
	public void disable() {
		enabled = false;
		service.shutdown();
	}

	@Override
	public boolean isEnabled() {
		return enabled;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("*** [PriorityTaskPool] Execution Report:\n");
		sb.append("Executions per operator: ").append(calls.toString()).append("\n");
		sb.append("Final States: \n");
		Operator<?, ?>[] tasksView = tasks.toArray(new Operator<?, ?>[0]);
		for (Operator<?, ?> task : tasksView) {
			sb.append(task.toString()).append(": ").append(metric.getPriority(task))
					.append(" " + QueueSizePriorityMetric.INSTANCE.getPriority(task)).append("\n");
		}
		sb.append("\n");
		return sb.toString();
	}

}
