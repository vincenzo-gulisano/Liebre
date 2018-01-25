package operator;

import java.util.Collection;

import common.StreamProducer;
import stream.Stream;

public enum QueueSizePriorityMetric implements PriorityMetric {
	INSTANCE;

	@Override
	public double getPriority(Operator<?, ?> operator) {
		Collection<StreamProducer<?>> previous = operator.getPrevious();
		double priority = 1.0;
		for (StreamProducer<?> prev : previous) {
			Stream<?> input = prev.getOutputStream(operator.getId());
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
