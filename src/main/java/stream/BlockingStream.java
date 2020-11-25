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
import component.StreamConsumer;
import component.StreamProducer;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockingStream<T> extends AbstractStream<T> {

  private static final double EMA_ALPHA = 0.01;
  private final BlockingQueue<T> stream;
  private final int capacity;
  private final StreamProducer<T> source;
  private final StreamConsumer<T> destination;
  private volatile long tuplesRead;
  private volatile long tuplesWritten;

  private volatile double averageArrivalTime = -1;

  /**
   * Construct.
   *  @param id The unique ID of the stream.
   * @param index The unique index of the stream.
   * @param source The producer
   * @param destination The consumer
   * @param capacity The capacity that the stream will enforce.
   */
  BlockingStream(
      String id,
      int index,
      StreamProducer<T> source,
      StreamConsumer<T> destination,
      int capacity) {
    super(id, index);
    this.capacity = capacity;
    this.stream = new ArrayBlockingQueue<>(capacity);
    this.source = source;
    this.destination = destination;
  }

  @Override
  public void doAddTuple(T tuple, int producerIndex) {
    try {
      stream.put(tuple);
      tuplesWritten++;
      if (tuple instanceof RichTuple) {
        long arrivalTime = ((RichTuple) tuple).getStimulus();
        averageArrivalTime =
            averageArrivalTime < 0
                ? arrivalTime
                : ((EMA_ALPHA * arrivalTime) + ((1 - EMA_ALPHA) * averageArrivalTime));
      }
    } catch (InterruptedException e) {
      disable();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public final boolean offer(T tuple, int producerIndex) {
    throw new UnsupportedOperationException("Use BackoffStream for non-blocking behavior");
  }

  @Override
  public T doGetNextTuple(int consumerIndex) {
    try {
      tuplesRead++;
      return stream.take();
    } catch (InterruptedException e) {
      if (isEnabled()) {
        disable();
      }
      Thread.currentThread().interrupt();
      return null;
    }
  }

  @Override
  public final T peek(int consumerIndex) {
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
  public List<? extends StreamProducer<T>> producers() {
  	//FIXME: Optimize
    return Arrays.asList(source);
  }

  @Override
  public List<? extends StreamConsumer<T>> consumers() {
    //FIXME: Optimize
    return Arrays.asList(destination);
  }

  @Override
  public void resetArrivalTime() {
    averageArrivalTime = -1;
  }

  @Override
  public double averageArrivalTime() {
    return averageArrivalTime;
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("flushing is not supported for blocking queues");
  }

  @Override
  public boolean isFlushed() {
    return false;
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

}
