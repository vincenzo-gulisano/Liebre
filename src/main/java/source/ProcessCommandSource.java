package source;

import common.tuple.Tuple;
import stream.Stream;

public class ProcessCommandSource<T extends Tuple> implements Runnable {
	private final Source<T> source;

	public ProcessCommandSource(Source<T> source) {
		this.source = source;
	}

	@Override
	public void run() {
		if (source.isEnabled()) {
			process();
		}
	}

	public final void process() {
		T tuple = source.getNextTuple();
		if (tuple != null) {
			Stream<T> output = source.getOutputStream(source.getId());
			output.addTuple(tuple);
			source.recordTupleWrite(tuple, output);
		}
	}

}
