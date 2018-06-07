package scheduling.priority;

import java.util.List;

import common.component.Component;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import sink.Sink;
import source.Source;
import stream.Stream;

public class QueueSizeMetric extends PriorityMetric {

	// TODO: Optimization where we record which boxes are "dirty" and update only
	// these priorities
	public QueueSizeMetric(List<Component> tasks, List<Component> ignoredTasks) {
		super(tasks, ignoredTasks);
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		long[] priorities = new long[tasks.size()];
		for (int i = 0; i < tasks.size(); i++) {
			priorities[i] = getPriority(tasks.get(i));
		}
		return scale(priorities, scaleFactor);
	}

	private long getPriority(Component task) {
		if (isIgnored(task)) {
			return 0;
		}
		if (task instanceof Source) {
			return getOutputCapacity((Source<?>) task);
		} else if (task instanceof Sink) {
			return getInputSize((Sink<?>) task);
		} else if (task instanceof Operator<?, ?>) {
			Operator<?, ?> operator = (Operator<?, ?>) task;
			return Math.min(getInputSize(operator), getOutputCapacity(operator));
		} else {
			throw new IllegalArgumentException("Cannot produce metric for class: " + task.getClass());
		}
	}

	private <IN extends Tuple> long getInputSize(StreamConsumer<IN> consumer) {
		long size = 0;
		for (Stream<?> input : getInputs(consumer)) {
			size += input.size();
		}
		return size;
	}

	private <OUT extends Tuple> long getOutputCapacity(StreamProducer<OUT> producer) {
		long capacity = 0;
		for (Stream<?> output : getOutputs(producer)) {
			capacity += output.remainingCapacity();
		}
		return capacity;
	}

}
