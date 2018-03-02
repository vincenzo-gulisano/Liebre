package scheduling.priority;

import java.util.List;

import common.exec.TimestampExecutionMatrix;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public class StimulusMatrixMetric extends PriorityMetric {

	private final TimestampExecutionMatrix matrix;

	public StimulusMatrixMetric(int nTasks, int nThreads) {
		this.matrix = new TimestampExecutionMatrix(nTasks, nThreads);
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		long[] timestamps = matrix.latest();
		preprocessTimestamps(timestamps);
		List<Double> res = scale(timestamps, scaleFactor);
		return res;
	}

	private void preprocessTimestamps(long[] timestamps) {
		long t = System.nanoTime();
		long defaultValue = t - minNonZero(timestamps);
		for (int i = 0; i < timestamps.length; i++) {
			long ts = timestamps[i];
			ts = (ts > 0) ? (t - ts) : defaultValue;
			timestamps[i] = ts;
		}
	}

	private long minNonZero(long[] values) {
		// TODO: This avoids starvation BUT
		// gives a non-zero priority to the sources
		// and might delay scheduling if we have many of them
		long min = Long.MAX_VALUE;
		for (int i = 0; i < values.length; i++) {
			long value = values[i];
			if (value > 0 && value < min) {
				min = value;
			}
		}
		return min;
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
		// TODO: Fail-fast behavior if not richtuples, maybe have to change at some
		// point
		matrix.put(threadIndex(), input.getSource().getIndex(), ((RichTuple) tuple).getTimestamp());
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
	}

}
