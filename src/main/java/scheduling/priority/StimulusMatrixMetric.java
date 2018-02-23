package scheduling.priority;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import common.ActiveRunnable;
import common.ExecutionMatrix;

//FIXME
public class StimulusMatrixMetric extends MatrixPriorityMetric {

	private final ExecutionMatrix matrix;

	public StimulusMatrixMetric(Map<String, Integer> index, int nTasks, int nThreads) {
		this.matrix = new ExecutionMatrix(index, nTasks, nThreads);
	}

	@Override
	public void updatePriorityStatistics(ActiveRunnable task, int threadId) {
		Map<String, Long> updates = task.getLatencyLog();
		matrix.updateReplace(updates, threadId);
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Long> stimulus = matrix.latest();
		long t = System.nanoTime();
		stimulus = stimulus.stream().map(i -> i == -1 ? 1 : t - i).collect(Collectors.toList());
		List<Double> res = scale(stimulus, scaleFactor);
		return res;
	}

}
