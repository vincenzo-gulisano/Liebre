package stream;

import common.StreamConsumer;
import common.StreamProducer;
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
  public <T extends Tuple> Stream<T> newBoundedStream(StreamProducer<T> from, StreamConsumer<T> to,
      int capacity) {
    Stream<T> stream = factory.newBoundedStream(from, to, capacity);
    return new StreamStatistic<>(stream, folder, autoFlush);
  }

}
