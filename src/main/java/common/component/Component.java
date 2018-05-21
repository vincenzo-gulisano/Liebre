package common.component;

import common.Active;
import common.Named;
import scheduling.priority.PriorityMetric;

public interface Component extends Active, Runnable, Named {
	void onScheduled();

	void onRun();

	void setPriorityMetric(PriorityMetric metric);

}
