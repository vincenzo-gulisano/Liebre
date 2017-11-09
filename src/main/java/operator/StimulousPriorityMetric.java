package operator;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import common.StreamProducer;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public enum StimulousPriorityMetric implements PriorityMetric {
	INSTANCE;
	private final Comparator<Operator<?, ?>> comparator = new Comparator<Operator<?, ?>>() {

		@Override
		public int compare(Operator<?, ?> o1, Operator<?, ?> o2) {
			return -Double.compare(getPriority(o1), getPriority(o2));
		}

	};

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
		return TimeUnit.NANOSECONDS.toMillis(diff);
	}

	@Override
	public Comparator<Operator<?, ?>> comparator() {
		return comparator;
	}

}
