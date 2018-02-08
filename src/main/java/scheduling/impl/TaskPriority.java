package scheduling.impl;

import common.ActiveRunnable;
import scheduling.priority.PriorityMetric;

public class TaskPriority implements Comparable<TaskPriority> {

	private final ActiveRunnable task;
	private final double priority;
	private final PriorityMetric metric;

	public static TaskPriority empty(ActiveRunnable task) {
		return new TaskPriority(task, 0, null);
	}

	public TaskPriority(ActiveRunnable task, PriorityMetric metric) {
		this(task, metric.getPriority(task), metric);
	}

	private TaskPriority(ActiveRunnable task, double priority, PriorityMetric metric) {
		this.task = task;
		this.priority = priority;
		this.metric = metric;
	}

	public ActiveRunnable getTask() {
		return task;
	}

	public double getPriority() {
		return priority;
	}

	@Override
	public int compareTo(TaskPriority o) {
		int m = metric.comparePriorities(priority, o.priority);
		if (m != 0) {
			return m;
		} else {
			return -String.CASE_INSENSITIVE_ORDER.compare(task.getId(), o.task.getId());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((task == null) ? 0 : task.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof TaskPriority))
			return false;
		TaskPriority other = (TaskPriority) obj;
		if (task == null) {
			if (other.task != null)
				return false;
		} else if (!task.equals(other.task))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return String.format("(%s, %3.2f, %s)", task, priority, metric);
	}

}
