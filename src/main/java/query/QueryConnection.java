package query;

import common.util.backoff.Backoff;
import component.StreamConsumer;
import component.StreamProducer;
import stream.Stream;

/**
 * Immutable snapshot of a query connection as it was declared through {@link Query}.
 */
public final class QueryConnection {

  public enum InputPort {
    DEFAULT,
    LEFT,
    RIGHT
  }

  private final StreamProducer<?> producer;
  private final StreamConsumer<?> consumer;
  private final InputPort inputPort;
  private final Stream<?> stream;
  private final Backoff backoff;

  QueryConnection(
      StreamProducer<?> producer,
      StreamConsumer<?> consumer,
      InputPort inputPort,
      Stream<?> stream,
      Backoff backoff) {
    this.producer = producer;
    this.consumer = consumer;
    this.inputPort = inputPort;
    this.stream = stream;
    this.backoff = backoff;
  }

  public StreamProducer<?> getProducer() {
    return producer;
  }

  public StreamConsumer<?> getConsumer() {
    return consumer;
  }

  public InputPort getInputPort() {
    return inputPort;
  }

  public Stream<?> getStream() {
    return stream;
  }

  public String getStreamId() {
    return stream.getId();
  }

  public Backoff getBackoff() {
    return backoff;
  }

  public boolean isSingleProducerSingleConsumer() {
    return stream.producers().size() == 1 && stream.consumers().size() == 1;
  }
}
