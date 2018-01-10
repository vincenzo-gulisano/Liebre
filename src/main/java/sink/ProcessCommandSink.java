package sink;

import common.tuple.Tuple;

public class ProcessCommandSink<T extends Tuple> implements Runnable {

	private final Sink<T> sink;

	public ProcessCommandSink(Sink<T> sink) {
		this.sink = sink;
	}

	@Override
	public void run() {
		while (sink.isEnabled()) {
			process();
		}

	}

	public final void process() {
		T t = sink.getInputStream(sink.getId()).getNextTuple();
		if (t != null) {
			sink.processTuple(t);
		}
	}

}
