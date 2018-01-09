package operator;

public enum CombinedPriorityMetric implements PriorityMetric {
	INSTANCE;

	@Override
	public double getPriority(Operator<?, ?> operator) {
		double queueSizePriority = QueueSizePriorityMetric.INSTANCE.getPriority(operator);
		double stimulusTime = StimulusPriorityMetric.INSTANCE.getPriority(operator);
		double total = stimulusTime == 0 ? queueSizePriority : queueSizePriority + (1 / stimulusTime);
		return total;
	}

	@Override
	public int comparePriorities(double p1, double p2) {
		return -Double.compare(p1, p2);
	}
}
