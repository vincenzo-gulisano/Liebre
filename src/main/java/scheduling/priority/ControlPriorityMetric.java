package scheduling.priority;

import java.util.List;
import java.util.Random;

import common.ActiveRunnable;
import common.tuple.Tuple;
import stream.Stream;

public class ControlPriorityMetric extends PriorityMetric {

	protected ControlPriorityMetric(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks) {
		super(tasks, passiveTasks);
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		Random rand = new Random();
		long[] priorities = new long[tasks.size()];
		for (int i = 0; i < tasks.size(); i++) {
			if (isIgnored(tasks.get(i))) {
				priorities[i] = 0;
				continue;
			}
			priorities[i] = rand.nextInt(100);

		}
		return scale(priorities, 1);
	}

}
