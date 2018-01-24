package query;

import common.tuple.Tuple;
import stream.BlockingStream;
import stream.Stream;
import stream.StreamFactory;

public enum BlockingStreamFactory implements StreamFactory {
	INSTANCE;
	@Override
	public <T extends Tuple> Stream<T> newStream(String fromId, String toId) {
		return new BlockingStream<>(String.format("%s_%s", fromId, toId));
	}

}
