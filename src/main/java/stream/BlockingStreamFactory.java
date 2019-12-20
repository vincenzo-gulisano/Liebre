package stream;

import component.StreamConsumer;
import component.StreamProducer;
import io.palyvos.dcs.common.util.backoff.BackoffFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

//TODO: Maybe split SG and regular stream factories?
public class BlockingStreamFactory implements StreamFactory {
  private final AtomicInteger indexes = new AtomicInteger();

  @Override
  public <T> Stream<T> newStream(
      StreamProducer<T> from, StreamConsumer<T> to, int capacity, BackoffFactory backoff) {
    return new BlockingStream<>(getStreamId(from, to),
        indexes.getAndIncrement(),
        from, to, capacity);
  }

  @Override
  public <T extends Comparable<? super T>> Stream<T> newMWMRSortedStream(
      List<StreamProducer<T>> sources, List<StreamConsumer<T>> destinations, int maxLevels) {
    return new SGStream<T>(getStreamId(sources.get(0), destinations.get(0)),
        indexes.getAndIncrement(),
        maxLevels, sources.size(),
        destinations.size(), sources, destinations);
  }
}
