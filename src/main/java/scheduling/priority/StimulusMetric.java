package scheduling.priority;

import java.util.List;

import common.component.Component;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public class StimulusMetric extends PriorityMetric {

	public StimulusMetric(List<Component> tasks, List<Component> ignoredTasks) {
		super(tasks, ignoredTasks);
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		long[] priorities = new long[tasks.size()];
		for (int i = 0; i < tasks.size(); i++) {
			priorities[i] = getPriority(tasks.get(i));
		}
		return scale(priorities, scaleFactor);
	}

	private long getPriority(Component task) {
		if (isIgnored(task)) {
			return 0;
		}
		long latency = 0;
		for (Stream<?> input : getInputs(task)) {
			// FIXME: Streams could save the latest ts in a volatile variable
			// to remove the peek() call
			Tuple t = input.peek();
			if (t instanceof RichTuple) {
				long ts = ((RichTuple) t).getTimestamp();
				latency = Math.max(System.nanoTime() - ts, latency);
			}
		}
		return latency;
	}

}
