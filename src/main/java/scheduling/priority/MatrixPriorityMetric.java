package scheduling.priority;

import java.util.ArrayList;
import java.util.List;

import common.ActiveRunnable;

public abstract class MatrixPriorityMetric {
	public abstract void updatePriorityStatistics(ActiveRunnable task, int threadId);

	public abstract List<Double> getPriorities(int scaleFactor);

	protected List<Double> scale(List<Long> data, int scaleFactor) {
		List<Double> scaled = new ArrayList<>(data.size());
		double sum = 0;
		for (int i = 0; i < data.size(); i++) {
			double d = data.get(i);
			sum += d;
			scaled.add(d);
		}
		normalize(scaled, sum);
		return scaled;
	}

	protected void normalize(List<Double> data, double sum) {
		for (int i = 0; i < data.size(); i++) {
			data.set(i, data.get(i) / sum);
		}
	}
}
