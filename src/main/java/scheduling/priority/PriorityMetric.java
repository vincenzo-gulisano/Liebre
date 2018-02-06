package scheduling.priority;

import java.util.Comparator;

import common.ActiveRunnable;

public interface PriorityMetric {
	double getPriority(ActiveRunnable task);

	int comparePriorities(double p1, double p2);

	default Comparator<ActiveRunnable> comparator() {
		return new Comparator<ActiveRunnable>() {

			@Override
			public int compare(ActiveRunnable t1, ActiveRunnable t2) {
				return comparePriorities(getPriority(t1), getPriority(t2));
			}
		};
	}
}
