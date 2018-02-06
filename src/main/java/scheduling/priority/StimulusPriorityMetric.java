package scheduling.priority;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public enum StimulusPriorityMetric implements PriorityMetric {
	INSTANCE;

	@Override
	public double getPriority(ActiveRunnable task) {
		if (task instanceof StreamConsumer == false) {
			return Double.NaN;
		}
		StreamConsumer<?> consumer = (StreamConsumer<?>) task;
		long latency = 0;
		// FIXME: Give a better interface for this
		for (StreamProducer<?> prev : consumer.getPrevious()) {
			Stream<?> input = prev.getOutputStream(consumer.getId());
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
