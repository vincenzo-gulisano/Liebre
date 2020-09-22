package stream;

import component.StreamConsumer;
import component.StreamProducer;
import common.util.backoff.Backoff;
import common.util.backoff.InactiveBackoff;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;

//TODO: Maybe split SG and regular stream factories?
public class BlockingStreamFactory implements StreamFactory {
  private final AtomicInteger indexes = new AtomicInteger();

  @Override
  public <T> Stream<T> newStream(
      StreamProducer<T> from, StreamConsumer<T> to, int capacity, Backoff backoff) {
    Validate.isTrue(backoff == InactiveBackoff.INSTANCE,
        "This stream does not support Back-off!");
    return new BlockingStream<>(getStreamId(from, to),
        indexes.getAndIncrement(),
        from, to, capacity);
  }

  @Override
  public <T extends Comparable<? super T>> MWMRStream<T> newMWMRStream(
          List<? extends StreamProducer<T>> sources, List<? extends StreamConsumer<T>> destinations, int maxLevels) {
    return new SGStream<T>(getStreamId(sources.get(0), destinations.get(0)),
        indexes.getAndIncrement(),
        maxLevels, sources.size(),
        destinations.size(), sources, destinations);
  }
}
