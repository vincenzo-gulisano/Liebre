package stream;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.Validate;

import common.tuple.RichTuple;
import common.tuple.Tuple;
import common.util.backoff.BackoffFactory;
import component.StreamConsumer;
import component.StreamProducer;

public class GlobalStreamFactory implements StreamFactory {

	private final AtomicInteger indexes = new AtomicInteger();

	@Override
	public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from,
			StreamConsumer<T> to, int relativeProducerIndex,
			int relativeConsumerIndex, int capacity, BackoffFactory backoff) {
		Validate.isTrue(backoff == BackoffFactory.NOOP,
				"This stream does not support Backoff!");
		return new BlockingStream<>(getStreamId(from, to),
				indexes.getAndIncrement(), relativeProducerIndex,
				relativeConsumerIndex, from, to, capacity);
	}

	@Override
	public <T extends RichTuple> MWMRSortedStream<T> newMWMRSortedStream(
			StreamProducer<T>[] sources, StreamConsumer<T>[] destinations,
			int relativeProducerIndex, int relativeConsumerIndex, int maxLevels) {
		return new SGStream<T>(getStreamId(sources[0], destinations[0]),
				indexes.getAndIncrement(), relativeProducerIndex,
				relativeConsumerIndex, maxLevels, sources.length,
				destinations.length, sources, destinations);
	}

}
