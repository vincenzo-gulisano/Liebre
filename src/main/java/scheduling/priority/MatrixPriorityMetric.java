package scheduling.priority;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import common.ActiveRunnable;

public abstract class MatrixPriorityMetric {
	public abstract void updatePriorityStatistics(ActiveRunnable task, Map<String, Integer> index, int threadId);

	public abstract List<Double> getPriorities(int scaleFactor);

	protected List<Double> scale(List<Long> data, int scaleFactor) {
		List<Double> scaled = new ArrayList<>(data.size());
		double sum = 0;
		int noValueCount = 0;
		for (int i = 0; i < data.size(); i++) {
			final long value = data.get(i);
			final double pi;
			if (value <= 0) {
				noValueCount++;
				pi = 0;
			} else {
				pi = Math.pow(data.get(i), scaleFactor);
			}
			scaled.add(pi);
			sum += pi;
		}
		// Set default priority for unknown metric values
		final double defaultValue = data.size() > noValueCount ? sum / (data.size() - noValueCount)
				: 1 / (double) data.size();
		sum = 0;
		for (int i = 0; i < scaled.size(); i++) {
			if (scaled.get(i) <= 0) {
				scaled.set(i, defaultValue);
			}
			sum += scaled.get(i);
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
