package query;

import java.util.Arrays;
import java.util.List;

import common.util.PropertyFileLoader;
import scheduling.priority.PriorityMetricFactory;

public class QueryConfiguration {
	private static final String SCHEDULING_ENABLED_KEY = "liebre.scheduling.enabled";
	private static final String SCHEDULING_INTERVAL_KEY = "liebre.scheduling.interval.nanos";

	private static final String NUMBER_THREADS_KEY = "liebre.scheduling.threads.worker.number";
	private static final String PRIORITY_METRIC_KEY = "liebre.scheduling.metric.name";
	private static final String TASK_POOL_TYPE_KEY = "liebre.scheduling.task.pool.type";
	private static final String PRIORITY_SCALING_FACTOR_KEY = "liebre.scheduling.priority.scaling";
	private static final String PRIORITY_INTERVAL_KEY = "liebre.scheduling.priority.interval.nanos";

	private final List<PriorityMetricFactory> availableMetrics = Arrays.asList(PriorityMetricFactory.values());

	private final boolean schedulingEnabled;
	private final int threadsNumber;
	private final long schedulingInterval;
	private final long priorityInterval;
	private final PriorityMetricFactory metricFactory;
	private final int taskPoolType;
	private final int priorityScalingFactor;

	private final PropertyFileLoader properties;

	public QueryConfiguration(String filename, Class<?> clazz) {
		this.properties = new PropertyFileLoader(filename, clazz);
		this.schedulingEnabled = Boolean.parseBoolean(getProperty(SCHEDULING_ENABLED_KEY));
		this.threadsNumber = Integer.parseInt(getProperty(NUMBER_THREADS_KEY));
		this.schedulingInterval = Long.parseLong(getProperty(SCHEDULING_INTERVAL_KEY));
		this.priorityInterval = Long.parseLong(getProperty(PRIORITY_INTERVAL_KEY));
		this.taskPoolType = Integer.parseInt(getProperty(TASK_POOL_TYPE_KEY));
		this.priorityScalingFactor = Integer.parseInt(getProperty(PRIORITY_SCALING_FACTOR_KEY));
		String priorityMetricName = getProperty(PRIORITY_METRIC_KEY);
		this.metricFactory = getObjectByName(priorityMetricName, availableMetrics);
	}

	private <T> T getObjectByName(String name, List<T> availableObjects) {
		T chosen = null;
		for (T obj : availableObjects) {
			boolean hasClassName = obj.getClass().getSimpleName().equals(name);
			boolean hasEnumMemberName = (obj instanceof Enum) && ((Enum<?>) obj).name().equals(name);
			if (hasClassName || hasEnumMemberName) {
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

	public long getPriorityInterval() {
		return priorityInterval;
	}

	public int getTaskPoolType() {
		return taskPoolType;
	}

	public int getPriorityScalingFactor() {
		return priorityScalingFactor;
	}

	public PriorityMetricFactory getMetricFactory() {
		return metricFactory;
	}

	@Override
	public String toString() {
		return "QueryConfiguration [schedulingEnabled=" + schedulingEnabled + ", threadsNumber=" + threadsNumber
				+ ", schedulingInterval=" + schedulingInterval + ", priorityInterval=" + priorityInterval
				+ ", priorityMetric=" + metricFactory + ", taskPoolType=" + taskPoolType + ", priorityScalingFactor="
				+ priorityScalingFactor + "]";
	}

}
