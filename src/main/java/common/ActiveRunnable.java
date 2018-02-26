package common;

import java.util.Map;

public interface ActiveRunnable extends Active, Runnable, NamedEntity {
	void onScheduled();

	void onRun();

	void enableExecutionMetrics();

	Map<String, Long> getInputQueueDiff();

	Map<String, Long> getOutputQueueDiff();

	Map<String, Long> getLatencyLog();
}
