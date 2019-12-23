package stream;

import component.StreamConsumer;
import component.StreamProducer;

public interface MWMRStream<T> extends Stream<T> {

  void registerProducer(StreamProducer<T> producer);

  void registerConsumer(StreamConsumer<T> consumer);
}
