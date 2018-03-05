package scheduling.priority;

import java.util.List;

import common.ActiveRunnable;
import common.exec.BaseExecutionMatrix;
import common.tuple.Tuple;
import stream.Stream;

public class QueueSizeMatrixMetric extends PriorityMetric {
	// FIXME: Get this from the streams
	private static final long QUEUE_CAPACITY = 10000;

	private final BaseExecutionMatrix streamMatrix;

	public QueueSizeMatrixMetric(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks, int nThreads) {
		super(tasks, passiveTasks, nThreads);
		this.streamMatrix = new BaseExecutionMatrix(maximumStreamIndex + 1, nThreads);
	}

	private long getTaskPriority(ActiveRunnable task, long[] streamValues) {
		if (isIgnored(task)) {
			return 0;
		}
		long priority = -1;
		for (int idx : getInputIndexes(task)) {
			long size = Math.max(streamValues[idx], 0);
			priority = priority < 0 ? size : Math.min(priority, size);
		}
		for (int idx : getOutputIndexes(task)) {
			// FIXME: Like above
			long capacity = Math.max(QUEUE_CAPACITY - streamValues[idx], 0);
			priority = Math.min(priority, capacity);
		}
		return priority;
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		long[] inputStreamSizes = streamMatrix.sum(0);
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
		streamMatrix.add(threadIndex(), input.getIndex(), -1L);
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		streamMatrix.add(threadIndex(), output.getIndex(), 1L);
	}

}
