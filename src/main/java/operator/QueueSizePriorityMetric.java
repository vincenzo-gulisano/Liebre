package operator;

import java.util.Collection;
import java.util.Comparator;

import common.StreamProducer;
import stream.Stream;

public enum QueueSizePriorityMetric implements PriorityMetric {
	INSTANCE;
	private final Comparator<Operator<?, ?>> comparator = new Comparator<Operator<?, ?>>() {

		@Override
		public int compare(Operator<?, ?> o1, Operator<?, ?> o2) {
			return -Double.compare(getPriority(o1), getPriority(o2));
		}

	};

	@Override
	public double getPriority(Operator<?, ?> operator) {
		Collection<StreamProducer<?>> previous = operator.getPrevious();
		double priority = 0;
		for (StreamProducer<?> prev : previous) {
			Stream<?> input = prev.getOutputStream(operator.getId());
			priority += input.size();
		}
		return priority;
	}

	@Override
	public Comparator<Operator<?, ?>> comparator() {
		return comparator;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

}
