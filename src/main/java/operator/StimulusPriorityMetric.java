package operator;

import common.StreamProducer;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public enum StimulusPriorityMetric implements PriorityMetric {
	INSTANCE;

	@Override
	public double getPriority(Operator<?, ?> operator) {
		long latency = 0;
		// FIXME: Give a better interface for this
		for (StreamProducer<?> prev : operator.getPrevious()) {
			Stream<?> input = prev.getOutputStream(operator.getId());
			Tuple t = input.peek();
			if (t instanceof RichTuple) {
				long ts = ((RichTuple) t).getTimestamp();
				latency = Math.max(System.nanoTime() - ts, latency);
			}
		}
		return 1 + latency;
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
