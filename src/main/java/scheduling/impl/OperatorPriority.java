package scheduling.impl;

import operator.Operator;
import scheduling.priority.PriorityMetric;

public class OperatorPriority implements Comparable<OperatorPriority> {

	private final Operator<?, ?> operator;
	private final double priority;
	private final PriorityMetric metric;

	public static OperatorPriority empty(Operator<?, ?> operator) {
		return new OperatorPriority(operator, 0, null);
	}

	public OperatorPriority(Operator<?, ?> operator, PriorityMetric metric) {
		this(operator, metric.getPriority(operator), metric);
	}

	private OperatorPriority(Operator<?, ?> operator, double priority, PriorityMetric metric) {
		this.operator = operator;
		this.priority = priority;
		this.metric = metric;
	}

	public Operator<?, ?> getOperator() {
		return operator;
	}

	public double getPriority() {
		return priority;
	}

	@Override
	public int compareTo(OperatorPriority o) {
		int m = metric.comparePriorities(priority, o.priority);
		if (m != 0) {
			return m;
		} else {
			return -String.CASE_INSENSITIVE_ORDER.compare(operator.getId(), o.operator.getId());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof OperatorPriority))
			return false;
		OperatorPriority other = (OperatorPriority) obj;
		if (operator == null) {
			if (other.operator != null)
				return false;
		} else if (!operator.equals(other.operator))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return String.format("(%s, %3.2f, %s)", operator, priority, metric);
	}

}
