package scheduling.priority;

import java.util.Map;

public enum MatrixMetricFactory {
	STIMULUS {
		@Override
		public MatrixPriorityMetric newInstance(Map<String, Integer> index, int nThreads) {
			return new StimulusMatrixMetric(index, index.size(), nThreads);
		}
	},
	QUEUE_SIZE {
		@Override
		public MatrixPriorityMetric newInstance(Map<String, Integer> index, int nThreads) {
			return new QueueSizeMatrixMetric(index, index.size(), nThreads);
		}
	};
	public abstract MatrixPriorityMetric newInstance(Map<String, Integer> index, int nThreads);
}
