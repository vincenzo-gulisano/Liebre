package scheduling.priority;

import java.util.List;

import common.tuple.Tuple;
import stream.Stream;

public class NoopPriorityMetric extends PriorityMetric {

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		return null;
	}

}
