package scheduling.priority;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import common.ActiveRunnable;
import common.ExecutionMatrix;
import common.StreamConsumer;
import common.StreamProducer;

public class QueueSizeMatrixMetric extends MatrixPriorityMetric {
	private static final long QUEUE_CAPACITY = 10000;

	private final ExecutionMatrix outputQueueMatrix;

	private final ExecutionMatrix inputQueueMatrix;

	public QueueSizeMatrixMetric(Map<String, Integer> index, int nTasks, int nThreads) {
		inputQueueMatrix = new ExecutionMatrix(index, nTasks, nThreads);
		outputQueueMatrix = new ExecutionMatrix(index, nTasks, nThreads);
	}

	@Override
	public void updatePriorityStatistics(ActiveRunnable task, int threadId) {
		if (task instanceof StreamProducer) {
			Map<String, Long> updates = ((StreamProducer<?>) task).getOutputDiff();
			if (updates.size() > 0 && task.getId().equals("R.test.00")) {
				// System.out.println("OUTPUT: " + task + " " + updates);
			}
			outputQueueMatrix.updateReplace(updates, threadId);
		}
		if (task instanceof StreamConsumer) {
			Map<String, Long> updates = ((StreamConsumer<?>) task).getInputQueueDiff();
			if (updates.size() > 0 && task.getId().equals("R.test.00")) {
				// System.out.println("INPUT: " + task + " " + updates);
			}
			inputQueueMatrix.updateReplace(updates, threadId);
		}
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Long> inputQueues = inputQueueMatrix.sum();
		List<Long> outputQueues = outputQueueMatrix.sum();
		System.out.println("INPUT\n" + inputQueueMatrix);
		System.out.println(inputQueues);
		System.out.println("OUTPUT\n" + outputQueueMatrix);
		System.out.println(outputQueues);
		List<Long> result = new ArrayList<>(inputQueues.size());
		for (int i = 0; i < inputQueues.size(); i++) {
			result.add(Math.min(QUEUE_CAPACITY - outputQueues.get(i), inputQueues.get(i)));
		}
		return scale(result, scaleFactor);
	}

}
