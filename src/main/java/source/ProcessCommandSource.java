package source;

import common.tuple.Tuple;

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
		T t = source.getNextTuple();
		if (t != null) {
			source.getOutputStream(source.getId()).addTuple(t);
		}
	}

}
