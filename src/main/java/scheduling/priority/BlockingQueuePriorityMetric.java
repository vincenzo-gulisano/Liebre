package scheduling.priority;

import java.util.Collection;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import sink.Sink;
import source.Source;
import stream.Stream;

public enum BlockingQueuePriorityMetric implements PriorityMetric {
	INSTANCE;

	@Override
	public double getPriority(ActiveRunnable task) {
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

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

	@Override
	public int comparePriorities(double p1, double p2) {
		return -Double.compare(p1, p2);
	}

}
