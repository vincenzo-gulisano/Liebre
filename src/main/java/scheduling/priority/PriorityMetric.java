package scheduling.priority;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import scheduling.thread.ActiveThread;
import stream.Stream;

public abstract class PriorityMetric {

	public abstract <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input);

	public abstract <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output);

	public abstract List<Double> getPriorities(int scaleFactor);

	protected List<Double> scale(long[] data, int scaleFactor) {
		List<Double> scaled = new ArrayList<>(data.length);
		double sum = 0;
		for (int i = 0; i < data.length; i++) {
			double d = data[i];
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

	protected List<Stream<?>> getInputs(ActiveRunnable task) {
		if (task instanceof StreamConsumer == false) {
			return Collections.emptyList();
		}
		List<Stream<?>> inputs = new ArrayList<>();
		StreamConsumer<?> consumer = (StreamConsumer<?>) task;
		for (StreamProducer<?> prev : consumer.getPrevious()) {
			Stream<?> input = prev.getOutputStream(consumer.getId());
			inputs.add(input);
		}
		return inputs;
	}

	protected List<Stream<?>> getOutputs(ActiveRunnable task) {
		if (task instanceof StreamProducer == false) {
			return Collections.emptyList();
		}
		StreamProducer<?> producer = (StreamProducer<?>) task;
		List<Stream<?>> outputs = new ArrayList<>();
		for (StreamConsumer<?> next : producer.getNext()) {
			Stream<?> output = next.getInputStream(producer.getId());
			outputs.add(output);
		}
		return outputs;
	}

}
