package scheduling.priority;

import java.util.Map;

public enum MatrixMetricFactory {
	STIMULUS {
		@Override
		public MatrixPriorityMetric newInstance(Map<String, Integer> index, int nTasks, int nThreads) {
			return new StimulusMatrixMetric(index, nTasks, nThreads);
		}
	},
	QUEUE_SIZE {
		@Override
		public MatrixPriorityMetric newInstance(Map<String, Integer> index, int nTasks, int nThreads) {
			return new QueueSizeMatrixMetric(index, nTasks, nThreads);
		}
	};
	public abstract MatrixPriorityMetric newInstance(Map<String, Integer> index, int nTasks, int nThreads);
}
