package scheduling.priority;

import java.util.ArrayList;
import java.util.List;

import common.ExecutionMatrix;
import common.tuple.Tuple;
import stream.Stream;

public class QueueSizeMatrixMetric extends MatrixPriorityMetric {
	private static final long QUEUE_CAPACITY = 10000;

	private final ExecutionMatrix outputQueueMatrix;

	private final ExecutionMatrix inputQueueMatrix;

	public QueueSizeMatrixMetric(int nTasks, int nThreads) {
		inputQueueMatrix = new ExecutionMatrix(nTasks, nThreads);
		outputQueueMatrix = new ExecutionMatrix(nTasks, nThreads);
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Long> inputQueues = inputQueueMatrix.sum(1L);
		List<Long> outputQueues = outputQueueMatrix.sum(1L);
		// System.out.println("INPUT\n" + inputQueueMatrix);
		// System.out.println(inputQueues);
		// System.out.println("OUTPUT\n" + outputQueueMatrix);
		// System.out.println(outputQueues);
		List<Long> result = new ArrayList<>(inputQueues.size());
		for (int i = 0; i < inputQueues.size(); i++) {
			result.add(Math.min(Math.max(QUEUE_CAPACITY - outputQueues.get(i), 1), inputQueues.get(i)));
		}
		return scale(result, scaleFactor);
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
		// FIXME: This should be in operator, e.g. NooppriorityMetric
		// if (!executionMetricsEnabled) {
		// return;
		// }
		if (tuple == null) {
			throw new IllegalStateException();
		}
		// When reading a tuple, the input queue size of this box
		// and the output queue of the previous one
		// is decreased
		int threadIndex = threadIndex();
		outputQueueMatrix.add(threadIndex, input.getSource().getIndex(), -1L);
		inputQueueMatrix.add(threadIndex, input.getDestination().getIndex(), -1L);
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		int threadIndex = threadIndex();
		outputQueueMatrix.add(threadIndex, output.getSource().getIndex(), 1L);
		inputQueueMatrix.add(threadIndex, output.getDestination().getIndex(), 1L);
	}

}
