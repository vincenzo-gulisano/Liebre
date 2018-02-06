package query;

import java.util.Arrays;
import java.util.List;

import common.util.PropertyFileLoader;
import scheduling.priority.BlockingQueuePriorityMetric;
import scheduling.priority.PriorityMetric;
import scheduling.priority.QueueSizePriorityMetric;
import scheduling.priority.StimulusPriorityMetric;

public class QueryConfiguration {
	private static final String SCHEDULING_ENABLED_KEY = "liebre.scheduling.enabled";
	private static final String SCHEDULING_INTERVAL_KEY = "liebre.scheduling.interval.millis";
	private static final String NUMBER_THREADS_KEY = "liebre.scheduling.threads.worker.number";
	private static final String NUMBER_HELPER_THREADS_KEY = "liebre.scheduling.threads.helper.number";
	private static final String HELPER_THREAD_INTERVAL_KEY = "liebre.scheduling.threads.helper.interval.millis";
	private static final String PRIORITY_METRIC_KEY = "liebre.scheduling.metric.name";
	private static final String TASK_POOL_TYPE = "liebre.scheduling.task.pool.type";
	private static final String PRIORITY_SCALING_FACTOR = "liebre.scheduling.priority.scaling";

	private final List<PriorityMetric> availableMetrics = Arrays.asList(QueueSizePriorityMetric.INSTANCE,
			StimulusPriorityMetric.INSTANCE, BlockingQueuePriorityMetric.INSTANCE);

	private final boolean schedulingEnabled;
	private final int threadsNumber;
	private final long schedulingInterval;
	private final int helperThreadsNumber;
	private final long helperThreadInterval;
	private final PriorityMetric priorityMetric;
	private final int taskPoolType;
	private final int priorityScalingFactor;

	private final PropertyFileLoader properties;

	public QueryConfiguration(String filename, Class<?> clazz) {
		properties = new PropertyFileLoader(filename, clazz);
		this.schedulingEnabled = Boolean.parseBoolean(getProperty(SCHEDULING_ENABLED_KEY));
		this.threadsNumber = Integer.parseInt(getProperty(NUMBER_THREADS_KEY));
		this.schedulingInterval = Long.parseLong(getProperty(SCHEDULING_INTERVAL_KEY));
		this.helperThreadsNumber = Integer.parseInt(getProperty(NUMBER_HELPER_THREADS_KEY));
		this.helperThreadInterval = Long.parseLong(getProperty(HELPER_THREAD_INTERVAL_KEY));
		this.taskPoolType = Integer.parseInt(getProperty(TASK_POOL_TYPE));
		this.priorityScalingFactor = Integer.parseInt(getProperty(PRIORITY_SCALING_FACTOR));
		String priorityMetricName = getProperty(PRIORITY_METRIC_KEY);
		this.priorityMetric = getObjectByName(priorityMetricName, availableMetrics);
	}

	private <T> T getObjectByName(String name, List<T> availableObjects) {
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

	/**
	 * Load a property either from the system properties or from the configuration
	 * file.
	 * 
	 * @param key
	 *            The property key
	 * @return The property value as {@link String}
	 */
	private String getProperty(String key) {
		return System.getProperty(key, properties.get(key));
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

	public int getTaskPoolType() {
		return taskPoolType;
	}

	public int getPriorityScalingFactor() {
		return priorityScalingFactor;
	}

	public PriorityMetric getPriorityMetric() {
		return priorityMetric;
	}

	@Override
	public String toString() {
		return "QueryConfiguration [schedulingEnabled=" + schedulingEnabled + ", threadsNumber=" + threadsNumber
				+ ", schedulingInterval=" + schedulingInterval + ", helperThreadsNumber=" + helperThreadsNumber
				+ ", helperThreadInterval=" + helperThreadInterval + ", priorityMetric=" + priorityMetric
				+ ", taskPoolType=" + taskPoolType + ", priorityScalingFactor=" + priorityScalingFactor + "]";
	}

}
