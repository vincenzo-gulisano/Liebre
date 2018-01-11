package query;

import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;
import stream.StreamStatistic;

public class ConcurrentLinkedListStreamStatisticFactory implements StreamFactory {

	private final String folder;
	private final boolean autoFlush;

	public ConcurrentLinkedListStreamStatisticFactory(String folder, boolean autoFlush) {
		this.folder = folder;
		this.autoFlush = autoFlush;
	}

	@Override
	public <T extends Tuple> Stream<T> newStream(String fromId, String toId) {
		Stream<T> stream = ConcurrentLinkedListStreamFactory.INSTANCE.newStream(fromId, toId);
		return new StreamStatistic<>(stream, folder, autoFlush);
	}

}
