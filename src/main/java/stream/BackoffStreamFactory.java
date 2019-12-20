package stream;

import io.palyvos.dcs.common.util.backoff.BackoffFactory;
import component.StreamConsumer;
import component.StreamProducer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;

public class BackoffStreamFactory implements StreamFactory {

	private final AtomicInteger indexes = new AtomicInteger();

	@Override
	public <T> Stream<T> newStream(StreamProducer<T> from,
			StreamConsumer<T> to,
			int capacity, BackoffFactory backoff) {
		Validate.isTrue(backoff == BackoffFactory.INACTIVE,
				"This stream does not support Backoff!");
		return new BackoffStream<>(
				getStreamId(from, to), indexes.getAndIncrement(), from, to, capacity, backoff);
	}

	@Override
	public <T extends Comparable<? super T>> Stream<T> newMWMRSortedStream(
			List<StreamProducer<T>> sources, List<StreamConsumer<T>> destinations,
			int maxLevels) {
		// TODO Ugly to get index 0 by default?
		return new SGStream<T>(getStreamId(sources.get(0), destinations.get(0)),
				indexes.getAndIncrement(),
				maxLevels, sources.size(),
				destinations.size(), sources, destinations);
	}

}
