package scheduling.priority;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import common.component.Component;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import scheduling.thread.LiebreThread;
import stream.Stream;

public abstract class PriorityMetric {

	protected final List<Component> tasks;
	protected final int maximumStreamIndex;
	private final Set<Integer> ignoredTasks;
	private final Map<String, List<Stream<?>>> inputStreams = new HashMap<>();
	private final Map<String, List<Stream<?>>> outputStreams = new HashMap<>();
	private final Map<String, List<Integer>> inputIndex = new HashMap<>();
	private final Map<String, List<Integer>> outputIndex = new HashMap<>();

	public static PriorityMetric noopMetric() {
		return new PriorityMetric() {

			@Override
			public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
			}

			@Override
			public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
			}

			@Override
			public List<Double> getPriorities(int scaleFactor) {
				throw new UnsupportedOperationException();
			}
		};
	}

	private PriorityMetric() {
		this.tasks = new ArrayList<>();
		this.ignoredTasks = new HashSet<>();
		this.maximumStreamIndex = -1;
	}

	protected PriorityMetric(List<Component> tasks, List<Component> passiveTasks) {
		this(tasks, passiveTasks, 0);
	}

	protected PriorityMetric(List<Component> tasks, List<Component> passiveTasks, int nThreads) {
		this.tasks = tasks;
		this.ignoredTasks = passiveTasks.stream().mapToInt(t -> t.getIndex()).boxed().collect(Collectors.toSet());
		for (Component task : tasks) {
			inputStreams.put(task.getId(), getInputs(task));
			outputStreams.put(task.getId(), getOutputs(task));
		}
		this.maximumStreamIndex = processStreamIndexes(tasks);
	}

	private int processStreamIndexes(List<Component> tasks) {
		int maximumStreamIndex = 0;
		for (Component task : tasks) {
			List<Integer> inputIndexes = getInputs(task).stream().mapToInt(s -> s.getIndex()).boxed()
					.collect(Collectors.toList());
			List<Integer> outputIndexes = getOutputs(task).stream().mapToInt(s -> s.getIndex()).boxed()
					.collect(Collectors.toList());
			inputIndex.put(task.getId(), inputIndexes);
			outputIndex.put(task.getId(), outputIndexes);
			maximumStreamIndex = Math.max(maximumStreamIndex, inputIndexes.stream().max(Integer::max).orElse(0));
			maximumStreamIndex = Math.max(maximumStreamIndex, outputIndexes.stream().max(Integer::max).orElse(0));
		}
		return maximumStreamIndex + 1;
	}

	public abstract <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input);

	public abstract <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output);

	public abstract List<Double> getPriorities(int scaleFactor);

	protected List<Double> exponential(long[] data, double lambda) {
		int[] sortedIndices = IntStream.range(0, data.length).boxed().sorted((i, j) -> Long.compare(data[i], data[j]))
				.mapToInt(ele -> ele).toArray();
		System.out.println(Arrays.toString(sortedIndices));
		List<Double> result = new ArrayList<Double>(Collections.nCopies(data.length, null));
		double sum = 0;
		for (int i = 0; i < data.length; i++) {
			int idx = sortedIndices[i];
			double value = lambda * Math.exp(-lambda * i);
			sum += value;
			result.set(idx, value);
		}
		System.out.println(result);
		normalize(result, sum);
		return result;
	}

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

	protected final int threadIndex() {
		return ((LiebreThread) Thread.currentThread()).getIndex();
	}

	protected final List<Stream<?>> getInputs(Component task) {
		if (task instanceof StreamConsumer == false) {
			return Collections.emptyList();
		}
		List<Stream<?>> cached = inputStreams.get(task.getId());
		if (cached != null) { // If cache has the content
			return cached;
		}
		List<Stream<?>> inputs = new ArrayList<>();
		StreamConsumer<?> consumer = (StreamConsumer<?>) task;
		for (StreamProducer<?> prev : consumer.getPrevious()) {
			Stream<?> input = prev.getOutputStream(consumer.getId());
			inputs.add(input);
		}
		return inputs;
	}

	protected final List<Integer> getInputIndexes(Component task) {
		return inputIndex.get(task.getId());
	}

	protected final List<Integer> getOutputIndexes(Component task) {
		return outputIndex.get(task.getId());
	}

	protected final List<Stream<?>> getOutputs(Component task) {
		if (task instanceof StreamProducer == false) {
			return Collections.emptyList();
		}
		List<Stream<?>> cached = outputStreams.get(task.getId());
		if (cached != null) { // If cache has the content
			return cached;
		}
		StreamProducer<?> producer = (StreamProducer<?>) task;
		List<Stream<?>> outputs = new ArrayList<>();
		for (StreamConsumer<?> next : producer.getNext()) {
			Stream<?> output = next.getInputStream(producer.getId());
			outputs.add(output);
		}
		return outputs;
	}

	protected final boolean isIgnored(Component task) {
		return ignoredTasks.contains(task.getIndex());
	}

	protected final boolean isIgnored(int taskIndex) {
		return ignoredTasks.contains(taskIndex);
	}

}
