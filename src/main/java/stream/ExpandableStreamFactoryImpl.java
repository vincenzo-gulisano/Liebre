package stream;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.Component;
import common.tuple.Tuple;
import java.util.concurrent.atomic.AtomicInteger;
import stream.smq.ExpandableStream;

public enum ExpandableStreamFactoryImpl implements StreamFactory {
  INSTANCE;

  private static final AtomicInteger indexes = new AtomicInteger();

  @Override
  public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
      int capacity) {
    return new ExpandableStream<>(new BoundedStream<>(getStreamId(from, to), indexes.getAndIncrement(), from, to,
        capacity));
  }

  private String getStreamId(Component from, Component to) {
    return String.format("%s_%s", from.getId(), to.getId());
  }

}
