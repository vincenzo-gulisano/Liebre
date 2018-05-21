package stream;

import common.component.Component;
import common.tuple.Tuple;

public class StreamStatisticFactory implements StreamFactory {

	private final String folder;
	private final boolean autoFlush;
	private final StreamFactory factory;

	public StreamStatisticFactory(StreamFactory factory, String folder, boolean autoFlush) {
		this.folder = folder;
		this.autoFlush = autoFlush;
		this.factory = factory;
	}

	@Override
	public <T extends Tuple> Stream<T> newStream(Component from, Component to) {
		Stream<T> stream = factory.newStream(from, to);
		return new StreamStatistic<>(stream, folder, autoFlush);
	}

}
