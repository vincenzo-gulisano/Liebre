package stream;

import common.component.Component;
import common.tuple.Tuple;

public enum BlockingStreamFactory implements StreamFactory {
	INSTANCE;
	@Override
	public <T extends Tuple> Stream<T> newStream(Component from, Component to) {
		return new BlockingStream<>(String.format("%s_%s", from.getId(), to.getId()), from, to);
	}

}
