package sink;

import common.tuple.Tuple;
import stream.Stream;

public class ProcessCommandSink<T extends Tuple> implements Runnable {

	private final Sink<T> sink;

	public ProcessCommandSink(Sink<T> sink) {
		this.sink = sink;
	}

	@Override
	public void run() {
		if (sink.isEnabled()) {
			process();
		}

	}

	public final void process() {
		Stream<T> input = sink.getInputStream(sink.getId());
		T tuple = input.getNextTuple();
		if (tuple != null) {
			sink.recordTupleRead(tuple, input);
			sink.processTuple(tuple);
		}
	}

}
