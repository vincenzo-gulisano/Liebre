package operator;

public interface PriorityMetric {
	double getPriority(Operator<?, ?> operator);
}
