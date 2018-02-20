package query;

import common.NamedEntity;
import common.tuple.Tuple;
import stream.BlockingStream;
import stream.Stream;
import stream.StreamFactory;

public enum BlockingStreamFactory implements StreamFactory {
	INSTANCE;
	@Override
	public <T extends Tuple> Stream<T> newStream(NamedEntity from, NamedEntity to) {
		return new BlockingStream<>(String.format("%s_%s", from.getId(), to.getId()), from, to);
	}

}
