package operator;

import java.util.Comparator;

public interface PriorityMetric {
	double getPriority(Operator<?, ?> operator);

	int comparePriorities(double p1, double p2);

	default Comparator<Operator<?, ?>> comparator() {
		return new Comparator<Operator<?, ?>>() {

			@Override
			public int compare(Operator<?, ?> o1, Operator<?, ?> o2) {
				return comparePriorities(getPriority(o1), getPriority(o2));
			}
		};
	}
}
