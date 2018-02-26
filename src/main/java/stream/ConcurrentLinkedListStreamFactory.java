package stream;

import common.ActiveRunnable;
import common.tuple.Tuple;

public enum ConcurrentLinkedListStreamFactory implements StreamFactory {
	INSTANCE;

	@Override
	public <T extends Tuple> Stream<T> newStream(ActiveRunnable from, ActiveRunnable to) {
		return new ConcurrentLinkedListStream<T>(String.format("%s_%s", from.getId(), to.getId()), from, to);
	}

}
