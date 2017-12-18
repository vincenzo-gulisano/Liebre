package query;

import java.util.Arrays;
import java.util.List;

import common.util.PropertyLoader;
import operator.CombinedPriorityMetric;
import operator.PriorityMetric;
import operator.QueueSizePriorityMetric;
import operator.StimulusPriorityMetric;

public class QueryConfiguration {
	private static final String SCHEDULING_ENABLED_KEY = "liebre.scheduling.enabled";
	private static final String SCHEDULING_INTERVAL_KEY = "liebre.scheduling.interval.millis";
	private static final String NUMBER_THREADS_KEY = "liebre.scheduling.threads.worker.number";
	private static final String NUMBER_HELPER_THREADS_KEY = "liebre.scheduling.threads.helper.number";
	private static final String HELPER_THREAD_INTERVAL_KEY = "liebre.scheduling.threads.helper.interval.millis";
	private static final String PRIORITY_METRIC_KEY = "liebre.scheduling.metric.name";

	private final List<PriorityMetric> availableMetrics = Arrays.asList(QueueSizePriorityMetric.INSTANCE,
			StimulusPriorityMetric.INSTANCE, CombinedPriorityMetric.INSTANCE);

	private final boolean schedulingEnabled;
	private final int threadsNumber;
	private final long schedulingInterval;
	private final int helperThreadsNumber;
	private final long helperThreadInterval;
	private final PriorityMetric priorityMetric;

	public QueryConfiguration(String filename, Class<?> clazz) {
		PropertyLoader properties = new PropertyLoader(filename, clazz);
		this.schedulingEnabled = Boolean.parseBoolean(properties.get(SCHEDULING_ENABLED_KEY));
		this.threadsNumber = Integer.parseInt(properties.get(NUMBER_THREADS_KEY));
		this.schedulingInterval = Long.parseLong(properties.get(SCHEDULING_INTERVAL_KEY));
		this.helperThreadsNumber = Integer.parseInt(properties.get(NUMBER_HELPER_THREADS_KEY));
		this.helperThreadInterval = Long.parseLong(properties.get(HELPER_THREAD_INTERVAL_KEY));
		String priorityMetricName = properties.get(PRIORITY_METRIC_KEY);
		this.priorityMetric = getObjectByName(priorityMetricName, availableMetrics, properties);
	}

	private <T> T getObjectByName(String name, List<T> availableObjects, PropertyLoader properties) {
		T chosen = null;
		for (T obj : availableObjects) {
			if (obj.getClass().getSimpleName().equals(name)) {
				chosen = obj;
				break;
			}
		}
		if (chosen == null) {
			throw new IllegalStateException(String.format("Object with name %s not available!", name));
		}
		return chosen;
	}

	public boolean isSchedulingEnabled() {
		return schedulingEnabled;
	}

	public int getThreadsNumber() {
		return threadsNumber;
	}

	public long getSchedulingInterval() {
		return schedulingInterval;
	}

	public int getHelperThreadsNumber() {
		return helperThreadsNumber;
	}

	public long getHelperThreadInterval() {
		return helperThreadInterval;
	}

	public PriorityMetric getPriorityMetric() {
		return priorityMetric;
	}

	@Override
	public String toString() {
		return "QueryConfiguration [schedulingEnabled=" + schedulingEnabled + ", threadsNumber=" + threadsNumber
				+ ", schedulingInterval=" + schedulingInterval + ", helperThreadsNumber=" + helperThreadsNumber
				+ ", helperThreadInterval=" + helperThreadInterval + ", priorityMetric=" + priorityMetric + "]";
	}

}
