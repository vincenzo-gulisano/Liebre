package operator;

import java.util.Comparator;

public interface PriorityMetric {
	double getPriority(Operator<?, ?> operator);

	Comparator<Operator<?, ?>> comparator();
}
