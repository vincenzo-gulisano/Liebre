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
import common.util.backoff.Backoff;
import common.util.backoff.ExponentialBackoff;
import common.util.backoff.InactiveBackoff;
import component.StreamConsumer;
import component.StreamProducer;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Stream implementation that has an (optional) approximate capacity that is enforced by an optional
 * provided {@link Backoff} strategy. The backoff strategy is also activated in case the reader is
 * faster than the writer, to prevent spinning.
 *
 * @param <T> The type of tuples transferred by this {@link Stream}.
 * @see StreamFactory
 * @see ExponentialBackoff
 * @see InactiveBackoff
 */
public class BackoffStream<T> extends AbstractStream<T> {

  private static final double EMA_ALPHA = 0.5;
  private final Queue<T> stream = new ConcurrentLinkedQueue<>();
  private final int capacity;
  private final StreamProducer<T> source;
  private final StreamConsumer<T> destination;
  private final Backoff readBackoff;
  private final Backoff writeBackoff;
  private volatile long tuplesRead;
  private volatile long tuplesWritten;
  private volatile boolean isFlushed = false;
  private volatile double averageArrivalTime = -1;

  /**
   * Construct.
   *  @param id The unique ID of the stream.
   * @param index The unique index of the stream.
   * @param source The producer
   * @param destination The consumer
   * @param capacity The capacity that the stream will try to enforce with the {@link Backoff}
*     strategy.
   * @param backoff The backoff strategy.
   */
  BackoffStream(
      String id,
      int index,
      StreamProducer<T> source,
      StreamConsumer<T> destination,
      int capacity,
      Backoff backoff) {
    super(id, index);
    this.capacity = capacity;
    this.source = source;
    this.destination = destination;
    this.readBackoff = backoff.newInstance();
    this.writeBackoff = backoff.newInstance();
  }

  @Override
  public void doAddTuple(T tuple, int producerIndex) {
    if (offer(tuple, producerIndex)) {
      writeBackoff.relax();
      return;
    }
    writeBackoff.backoff();
  }

  @Override
  public final boolean offer(T tuple, int producerIndex) {
    stream.offer(tuple);
    tuplesWritten++;
    // FIXME: This should only run when scheduling is enabled!!
    if (tuple instanceof RichTuple) {
      long arrivalTime = ((RichTuple) tuple).getStimulus();
      averageArrivalTime =
          averageArrivalTime < 0
              ? arrivalTime
              : ((EMA_ALPHA * arrivalTime) + ((1 - EMA_ALPHA) * averageArrivalTime));
    }
    return remainingCapacity() > 0;
  }

  @Override
  public T doGetNextTuple(int consumerIndex) {
    T tuple = stream.poll();
    if (tuple != null) {
      readBackoff.relax();
      tuplesRead++;
      return tuple;
    }
    readBackoff.backoff();
    return null;
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
    // FIXME: Optimize
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
    isFlushed = true;
  }

  @Override
  public boolean isFlushed() {
    return isFlushed && stream.isEmpty();
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

  @Override
  public void clear() {
    stream.clear();
  }

}
