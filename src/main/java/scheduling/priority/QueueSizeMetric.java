package scheduling.priority;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import sink.Sink;
import source.Source;
import stream.Stream;

public class QueueSizeMetric extends PriorityMetric {
	private final List<ActiveRunnable> tasks;
	private final Set<Integer> ignoredIndexes;

	// TODO: Optimization where we record which boxes are "dirty" and update only
	// these priorities
	public QueueSizeMetric(List<ActiveRunnable> tasks, List<ActiveRunnable> ignoredTasks) {
		this.tasks = Collections.unmodifiableList(tasks);
		Set<Integer> passiveIndexes = new HashSet<>();
		for (ActiveRunnable task : ignoredTasks) {
			passiveIndexes.add(task.getIndex());
		}
		this.ignoredIndexes = Collections.unmodifiableSet(passiveIndexes);
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {

	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Long> priorities = new ArrayList<>(tasks.size());
		for (ActiveRunnable task : tasks) {
			priorities.add(getPriority(task));
		}
		return scale(priorities, scaleFactor);
	}

	public long getPriority(ActiveRunnable task) {
		if (ignoredIndexes.contains(task.getIndex())) {
			return 0;
		}
		if (task instanceof Source) {
			return getMinimumOutputCapacity((Source<?>) task);
		} else if (task instanceof Sink) {
			return getMinimumInputSize((Sink<?>) task);
		} else if (task instanceof Operator<?, ?>) {
			Operator<?, ?> operator = (Operator<?, ?>) task;
			return Math.min(getMinimumInputSize(operator), getMinimumOutputCapacity(operator));
		} else {
			throw new IllegalArgumentException("Cannot produce metric for class: " + task.getClass());
		}
	}

	private <IN extends Tuple> long getMinimumInputSize(StreamConsumer<IN> consumer) {
		long minInputSize = -1;
		Collection<StreamProducer<?>> previous = consumer.getPrevious();
		for (StreamProducer<?> prev : previous) {
			Stream<?> input = prev.getOutputStream(consumer.getId());
			minInputSize = minInputSize < 0 ? input.size() : Math.min(input.size(), minInputSize);
		}
		return minInputSize;
	}

	private <OUT extends Tuple> long getMinimumOutputCapacity(StreamProducer<OUT> producer) {
		long minOutputCapacity = -1;
		Collection<StreamConsumer<OUT>> nextOperators = producer.getNext();
		for (StreamConsumer<?> next : nextOperators) {
			Stream<?> output = next.getInputStream(producer.getId());
			minOutputCapacity = minOutputCapacity < 0 ? output.remainingCapacity()
					: Math.min(output.remainingCapacity(), minOutputCapacity);
		}
		return minOutputCapacity;
	}

}
