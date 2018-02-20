package scheduling.priority;

public enum MatrixMetricFactory {
	STIMULUS {
		@Override
		public MatrixPriorityMetric newInstance(int nThreads, int nTasks) {
			return new StimulusMatrixMetric(nTasks, nThreads);
		}
	},
	QUEUE_SIZE {
		@Override
		public MatrixPriorityMetric newInstance(int nTasks, int nThreads) {
			return new QueueSizeMatrixMetric(nTasks, nThreads);
		}
	};
	public abstract MatrixPriorityMetric newInstance(int nTasks, int nThreads);
}
