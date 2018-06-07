package stream;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;

public interface StreamFactory {

  <T extends Tuple> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
      int capacity);

}
