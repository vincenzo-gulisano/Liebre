package operator;

import java.util.Collection;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;

public enum BlockingQueuePriorityMetric implements PriorityMetric {
	INSTANCE;

	private <IN extends Tuple, OUT extends Tuple> long getMinimumInputSjze(Operator<IN, OUT> operator) {
		long minInputSize = -1;
		Collection<StreamProducer<?>> previous = operator.getPrevious();
		for (StreamProducer<?> prev : previous) {
			Stream<?> input = prev.getOutputStream(operator.getId());
			minInputSize = minInputSize < 0 ? input.size() : Math.min(input.size(), minInputSize);
		}
		return minInputSize;
	}

	private <IN extends Tuple, OUT extends Tuple> long getMinimumOutputCapacity(Operator<IN, OUT> operator) {
		long minOutputCapacity = -1;
		Collection<StreamConsumer<OUT>> nextOperators = operator.getNext();
		for (StreamConsumer<?> next : nextOperators) {
			Stream<?> output = next.getInputStream(operator.getId());
			minOutputCapacity = minOutputCapacity < 0 ? output.remainingCapacity()
					: Math.min(output.remainingCapacity(), minOutputCapacity);
		}
		return minOutputCapacity;
	}

	@Override
	public double getPriority(Operator<?, ?> operator) {
		return Math.min(getMinimumInputSjze(operator), getMinimumOutputCapacity(operator));
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
