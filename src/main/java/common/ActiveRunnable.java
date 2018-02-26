package common;

import scheduling.priority.MatrixPriorityMetric;

public interface ActiveRunnable extends Active, Runnable, NamedEntity {
	void onScheduled();

	void onRun();

	void setPriorityMetric(MatrixPriorityMetric metric);

}
