package common;

import java.util.Map;

public interface ActiveRunnable extends Active, Runnable, NamedEntity {
	void onScheduled();

	void onRun();

	Map<String, Long> getInputQueueDiff();

	Map<String, Long> getOutputQueueDiff();

	Map<String, Long> getLatencyLog();
}
