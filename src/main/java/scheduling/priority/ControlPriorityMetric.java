package scheduling.priority;

import java.util.List;
import java.util.Random;

import common.ActiveRunnable;

public class ControlPriorityMetric extends QueueSizeMetric {

	protected ControlPriorityMetric(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks) {
		super(tasks, passiveTasks);
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Double> priorities = super.getPriorities(scaleFactor);
		Random r = new Random();
		double sum = 0;
		for (int i = 0; i < priorities.size(); i++) {
			double p = isIgnored(tasks.get(i)) ? 0 : r.nextDouble();
			priorities.set(i, p);
			sum += p;
		}
		normalize(priorities, sum);
		return priorities;
	}

}
