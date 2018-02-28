package common;

import scheduling.priority.PriorityMetric;

public interface ActiveRunnable extends Active, Runnable, NamedEntity {
	void onScheduled();

	void onRun();

	void setPriorityMetric(PriorityMetric metric);

}
