package query;

import common.tuple.Tuple;
import stream.ConcurrentLinkedListStream;
import stream.Stream;
import stream.StreamFactory;

public enum ConcurrentLinkedListStreamFactory implements StreamFactory {
	INSTANCE;

	@Override
	public <T extends Tuple> Stream<T> newStream(String fromId, String toId) {
		return new ConcurrentLinkedListStream<T>(String.format("%s_%s", fromId, toId));
	}

}
