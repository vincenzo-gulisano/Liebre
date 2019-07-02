/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package stream;

import common.tuple.RichTuple;
import common.tuple.Tuple;
import common.util.backoff.BackoffFactory;
import component.StreamConsumer;
import component.StreamProducer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BlockingStream<T extends Tuple> implements SSSRStream<T> {

  private static final double EMA_ALPHA = 0.01;
  private final BlockingQueue<T> stream;
  private final int capacity;
  private final String id;
  private final int index;
  private final StreamProducer<T> source;
  private final StreamConsumer<T> destination;
  private volatile boolean enabled;
  private volatile long tuplesRead;
  private volatile long tuplesWritten;

  private volatile double averageArrivalTime = -1;

  /**
   * Construct.
   *
   * @param id The unique ID of the stream.
   * @param index The unique index of the stream.
   * @param source The producer
   * @param destination The consumer
   * @param capacity The capacity that the stream will enforce.
   */
  BlockingStream(String id, int index, StreamProducer<T> source,
      StreamConsumer<T> destination,
      int capacity) {
    this.capacity = capacity;
    this.stream = new ArrayBlockingQueue<>(capacity);
    this.id = id;
    this.index = index;
    this.source = source;
    this.destination = destination;
  }

  /**
   * Get a {@link StreamFactory} implementation for {@link BackoffStream}s.
   *
   * @return A factory that generates {@link BackoffStream}s.
   */
  public static StreamFactory factory() {
    return BlockingStream.Factory.INSTANCE;
  }

  @Override
  public void addTuple(T tuple) {
    try {
      stream.put(tuple);
      if (tuple instanceof RichTuple) {
        long arrivalTime = ((RichTuple) tuple).getStimulus();
        averageArrivalTime = averageArrivalTime < 0 ? arrivalTime :
            ((EMA_ALPHA * arrivalTime) + ((1 - EMA_ALPHA) * averageArrivalTime));
      }
    } catch (InterruptedException e) {
      disable();
      Thread.currentThread().interrupt();
    }
  }


  @Override
  public final boolean offer(T tuple) {
    throw new UnsupportedOperationException("Use BackoffStream for non-blocking behavior");
  }

  @Override
  public T getNextTuple() {
    try {
      return stream.take();
    } catch (InterruptedException e) {
      disable();
      Thread.currentThread().interrupt();
      return null;
    }
  }

  @Override
  public final T poll() {
    throw new UnsupportedOperationException("Use BackoffStream for non-blocking behavior");
  }

  @Override
  public final T peek() {
    return stream.peek();
  }

  @Override
  public final int remainingCapacity() {
    return Math.max(capacity - size(), 0);
  }

  @Override
  public final int size() {
    return (int) (tuplesWritten - tuplesRead);
  }

  @Override
  public StreamProducer<T> getSource() {
    return source;
  }

  @Override
  public StreamConsumer<T> getDestination() {
    return destination;
  }

  @Override
  public void resetArrivalTime() {
    averageArrivalTime = -1;
  }

  @Override
  public double getAverageArrivalTime() {
    return averageArrivalTime;
  }

  @Override
  public void enable() {
    enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    enabled = false;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("index", index)
        .append("capacity", capacity)
        .append("size", size())
        .append("component/source", source)
        .append("destination", destination)
        .append("enabled", enabled)
        .toString();
  }

  /**
   * Default factory implementation, that enforces unique increasing integer indexes.
   */
  private enum Factory implements StreamFactory {
    INSTANCE;

    private final AtomicInteger indexes = new AtomicInteger();

    @Override
    public <T extends Tuple> SSSRStream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
        int capacity, BackoffFactory backoff) {
      Validate.isTrue(backoff == BackoffFactory.NOOP, "This stream does not support Backoff!");
      return new BlockingStream<>(getStreamId(from, to), indexes.getAndIncrement(), from, to,
          capacity);
    }
  }
}
