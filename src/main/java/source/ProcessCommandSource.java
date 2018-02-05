package source;

import common.tuple.Tuple;

public class ProcessCommandSource<T extends Tuple> implements Runnable {
	private final Source<T> source;

	public ProcessCommandSource(Source<T> source) {
		this.source = source;
	}

	@Override
	public void run() {
		while (source.isEnabled()) {
			process();
		}
	}

	public final void process() {
		T t = source.getNextTuple();
		source.onScheduled();
		if (t != null) {
			source.onRun();
			source.getOutputStream(source.getId()).addTuple(t);
		}
	}

}
