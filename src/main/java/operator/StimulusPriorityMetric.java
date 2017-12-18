package operator;

import common.StreamProducer;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public enum StimulusPriorityMetric implements PriorityMetric {
	INSTANCE;

	@Override
	public double getPriority(Operator<?, ?> operator) {
		long diff = 0;
		// FIXME: Give a better interface for this
		for (StreamProducer<?> prev : operator.getPrevious()) {
			Stream<?> input = prev.getOutputStream(operator.getId());
			Tuple t = input.peek();
			if (t instanceof RichTuple) {
				long ts = ((RichTuple) t).getTimestamp();
				diff = Math.max(System.nanoTime() - ts, diff);
			}
		}
		return diff;
	}

	@Override
	public int comparePriorities(double p1, double p2) {
		return -Double.compare(p1, p2);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

}
