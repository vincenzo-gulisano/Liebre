package scheduling.priority;

import java.util.ArrayList;
import java.util.List;

import common.tuple.Tuple;
import scheduling.thread.ActiveThread;
import stream.Stream;

public abstract class PriorityMetric {

	public abstract <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input);

	public abstract <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output);

	public abstract List<Double> getPriorities(int scaleFactor);

	protected List<Double> scale(List<Long> data, int scaleFactor) {
		List<Double> scaled = new ArrayList<>(data.size());
		double sum = 0;
		for (int i = 0; i < data.size(); i++) {
			double d = data.get(i);
			sum += d;
			scaled.add(d);
		}
		normalize(scaled, sum);
		return scaled;
	}

	protected void normalize(List<Double> data, double sum) {
		for (int i = 0; i < data.size(); i++) {
			data.set(i, data.get(i) / sum);
		}
	}

	protected int threadIndex() {
		return ((ActiveThread) Thread.currentThread()).getIndex();
	}
}
