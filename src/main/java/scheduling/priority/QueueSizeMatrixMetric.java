package scheduling.priority;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import common.ActiveRunnable;
import common.ExecutionMatrix;
import common.StreamConsumer;
import common.StreamProducer;

public class QueueSizeMatrixMetric extends MatrixPriorityMetric {
	private final ExecutionMatrix writeMatrix;

	private final ExecutionMatrix readMatrix;

	public QueueSizeMatrixMetric(int nTasks, int nThreads) {
		readMatrix = new ExecutionMatrix(nTasks, nThreads);
		writeMatrix = new ExecutionMatrix(nTasks, nThreads);
	}

	@Override
	public void updatePriorityStatistics(ActiveRunnable task, Map<String, Integer> index, int threadId) {
		if (task instanceof StreamProducer) {
			Map<String, Long> updates = ((StreamProducer<?>) task).getWriteLog();
			writeMatrix.update(updates, index, threadId);
		} else if (task instanceof StreamConsumer) {
			Map<String, Long> updates = ((StreamConsumer<?>) task).getReadLog();
			readMatrix.update(updates, index, threadId);
		}
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Long> reads = readMatrix.sum();
		List<Long> writes = writeMatrix.sum();
		List<Long> result = new ArrayList<>(reads.size());
		for (int i = 0; i < reads.size(); i++) {
			result.add(writes.get(i) - reads.get(i));
		}
		return scale(result, scaleFactor);
	}

}
