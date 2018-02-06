package scheduling.priority;

import java.util.Collection;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import stream.Stream;

public enum QueueSizePriorityMetric implements PriorityMetric {
	INSTANCE;

	@Override
	public double getPriority(ActiveRunnable task) {
		if (task instanceof StreamConsumer == false) {
			return Double.NaN;
		}
		StreamConsumer<?> consumer = (StreamConsumer<?>) task;
		Collection<StreamProducer<?>> previous = consumer.getPrevious();
		double priority = 1.0;
		for (StreamProducer<?> prev : previous) {
			Stream<?> input = prev.getOutputStream(consumer.getId());
			priority += input.size();
		}
		return priority;
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
