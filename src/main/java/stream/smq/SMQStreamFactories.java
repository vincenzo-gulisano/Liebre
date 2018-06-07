package stream.smq;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.BoundedStreamFactory;
import stream.Stream;
import stream.StreamFactory;

public enum SMQStreamFactories implements StreamFactory {
  EXPANDABLE {
    @Override
    public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
        int capacity) {
      return new ExpandableStream<>(BoundedStreamFactory.INSTANCE.newStream(from, to, capacity));
    }
  },
  NOTIFYING {
    @Override
    public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
        int capacity) {
      return new NotifyingStream<>(BoundedStreamFactory.INSTANCE.newStream(from, to, capacity));
    }
  };



}
