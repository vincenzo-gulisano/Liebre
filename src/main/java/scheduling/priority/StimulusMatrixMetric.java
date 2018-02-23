package scheduling.priority;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import common.ActiveRunnable;
import common.ExecutionMatrix;
import common.StreamProducer;

//FIXME
public class StimulusMatrixMetric extends MatrixPriorityMetric {

	private final ExecutionMatrix matrix;

	public StimulusMatrixMetric(Map<String, Integer> index, int nTasks, int nThreads) {
		this.matrix = new ExecutionMatrix(index, nTasks, nThreads);
		matrix.init(Long.MAX_VALUE);
	}

	@Override
	public void updatePriorityStatistics(ActiveRunnable task, int threadId) {
		if (task instanceof StreamProducer == false) {
			return;
		}
		Map<String, Long> log = ((StreamProducer<?>) task).getLatencyLog();
		matrix.updateReplace(log, threadId);
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Long> stimulus = matrix.min();
		long t = System.nanoTime();
		stimulus = stimulus.stream().map(i -> i == Long.MAX_VALUE ? 0 : t - i).collect(Collectors.toList());
		List<Double> res = scale(stimulus, scaleFactor);
		return res;
	}

}
