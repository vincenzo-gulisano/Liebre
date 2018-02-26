package scheduling.priority;

import java.util.List;
import java.util.stream.Collectors;

import common.ExecutionMatrix;
import common.tuple.Tuple;
import stream.Stream;

//FIXME
public class StimulusMatrixMetric extends MatrixPriorityMetric {

	private final ExecutionMatrix matrix;

	public StimulusMatrixMetric(int nTasks, int nThreads) {
		this.matrix = new ExecutionMatrix(nTasks, nThreads);
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		List<Long> stimulus = matrix.latest();
		long t = System.nanoTime();
		stimulus = stimulus.stream().map(i -> i == -1 ? 1 : t - i).collect(Collectors.toList());
		List<Double> res = scale(stimulus, scaleFactor);
		return res;
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
		throw new UnsupportedOperationException();
		// FIXME: Implement
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		throw new UnsupportedOperationException();
		// FIXME: Implement

	}

}
