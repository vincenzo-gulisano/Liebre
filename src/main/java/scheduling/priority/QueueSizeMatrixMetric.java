package scheduling.priority;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import common.ActiveRunnable;
import common.exec.BaseExecutionMatrix;
import common.tuple.Tuple;
import stream.Stream;

public class QueueSizeMatrixMetric extends PriorityMetric {
	private static final long QUEUE_CAPACITY = 10000;

	// private final BaseExecutionMatrix outputQueueMatrix;
	private final BaseExecutionMatrix inputStreamMatrix;
	private final List<ActiveRunnable> tasks;
	private final Map<String, List<Integer>> operatorStreams = new HashMap<>();

	public QueueSizeMatrixMetric(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks, int nThreads) {
		final int maximumIndex = maximumStreamIndex(getInputs(tasks), getOutputs(tasks));
		this.inputStreamMatrix = new BaseExecutionMatrix(maximumIndex + 1, nThreads);
		this.tasks = tasks;
		initStreamIndexes(tasks);
		// outputQueueMatrix = new BaseExecutionMatrix(nTasks, nThreads);
	}

	private void initStreamIndexes(List<ActiveRunnable> tasks) {
		for (ActiveRunnable task : tasks) {
			List<Integer> indexes = getInputs(task).stream().mapToInt(s -> s.getIndex()).boxed()
					.collect(Collectors.toList());
			operatorStreams.put(task.getId(), indexes);
		}
	}

	private List<Stream<?>> getInputs(List<ActiveRunnable> tasks) {
		List<Stream<?>> streams = new ArrayList<>();
		for (ActiveRunnable task : tasks) {
			streams.addAll(getInputs(task));
		}
		return streams;
	}

	private List<Stream<?>> getOutputs(List<ActiveRunnable> tasks) {
		List<Stream<?>> streams = new ArrayList<>();
		for (ActiveRunnable task : tasks) {
			streams.addAll(getOutputs(task));
		}
		return streams;
	}

	@SafeVarargs
	private final int maximumStreamIndex(List<Stream<?>>... streamLists) {
		int maxIndex = 0;
		for (List<Stream<?>> streams : streamLists) {
			for (Stream<?> stream : streams) {
				maxIndex = stream.getIndex() > maxIndex ? stream.getIndex() : maxIndex;
			}
		}
		return maxIndex;
	}

	private long getTaskPriority(ActiveRunnable task, long[] streamValues) {
		long sum = 0;
		for (int idx : operatorStreams.get(task.getId())) {
			sum += streamValues[idx];
		}
		return sum;
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		long[] inputStreamSizes = inputStreamMatrix.sum(0);
		long[] priorities = new long[tasks.size()];
		for (ActiveRunnable task : tasks) {
			priorities[task.getIndex()] = getTaskPriority(task, inputStreamSizes);

		}
		return scale(priorities, scaleFactor);
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
		if (tuple == null) {
			throw new IllegalStateException();
		}
		// When reading a tuple, the input queue size of this box
		// and the output queue of the previous one
		// is decreased
		int threadIndex = threadIndex();
		// outputQueueMatrix.add(threadIndex, input.getSource().getIndex(), -1L);
		inputStreamMatrix.add(threadIndex, input.getIndex(), -1L);
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		int threadIndex = threadIndex();
		// outputQueueMatrix.add(threadIndex, output.getSource().getIndex(), 1L);
		inputStreamMatrix.add(threadIndex, output.getIndex(), 1L);
	}

}
