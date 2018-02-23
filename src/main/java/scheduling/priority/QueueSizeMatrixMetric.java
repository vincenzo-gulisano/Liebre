package scheduling.priority;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import common.ActiveRunnable;
import common.ExecutionMatrix;

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
		Map<String, Long> outputUpdates = task.getOutputQueueDiff();
		Map<String, Long> inputUpdates = task.getInputQueueDiff();
		// if (inputUpdates.size() > 0 && task.getId().startsWith("I")) {
		// System.out.format("[T%d] INPUT %s %s%n", threadId, task.getId(),
		// inputUpdates);
		// }
		inputQueueMatrix.updateApply(inputUpdates, threadId, (a, b) -> a + b);
		outputQueueMatrix.updateApply(outputUpdates, threadId, (a, b) -> a + b);
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

}
