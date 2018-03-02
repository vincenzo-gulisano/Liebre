package scheduling.priority;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

//FIXME: Retrieval of input/output streams of each task can be done
//as a preprocessing step
public class StimulusMetric extends PriorityMetric {
	private final List<ActiveRunnable> tasks;
	private final Set<Integer> ignoredIndexes;

	public StimulusMetric(List<ActiveRunnable> tasks, List<ActiveRunnable> ignoredTasks) {
		this.tasks = Collections.unmodifiableList(tasks);
		Set<Integer> passiveIndexes = new HashSet<>();
		for (ActiveRunnable task : ignoredTasks) {
			passiveIndexes.add(task.getIndex());
		}
		this.ignoredIndexes = Collections.unmodifiableSet(passiveIndexes);
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

	private long getPriority(ActiveRunnable task) {
		if (ignoredIndexes.contains(task.getIndex()) || task instanceof StreamConsumer == false) {
			return 0;
		}
		StreamConsumer<?> consumer = (StreamConsumer<?>) task;
		long latency = 0;
		// FIXME: Give a better interface for this
		for (Stream<?> input : getInputs(consumer)) {
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
