/*
 * Copyright (C) 2017-2018
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

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import common.util.backoff.Backoff;
import common.util.backoff.Backoff2;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.builder.ToStringBuilder;

//FIXME: Custom backoff!
public class UnboundedStream<T extends Tuple> implements Stream<T> {

  private final Queue<T> stream = new ConcurrentLinkedQueue<>();
  private final int capacity;
  private final String id;
  private final int index;
  private final StreamProducer<T> source;
  private final StreamConsumer<T> destination;
  private final Backoff readBackoff = new Backoff2(1, 20, 5);
  private final Backoff writeBackoff = new Backoff2(1, 20, 5);
  private volatile boolean enabled;
  private volatile long tupleRead;
  private volatile long tuplesWritten;

  public UnboundedStream(String id, int index, StreamProducer<T> source,
      StreamConsumer<T> destination,
      int capacity) {
    this.capacity = capacity;
    this.id = id;
    this.index = index;
    this.source = source;
    this.destination = destination;

  }

  public static StreamFactory factory() {
    return Factory.INSTANCE;
  }

  @Override
  public void addTuple(T tuple) {
    if (offer(tuple)) {
      writeBackoff.relax();
      return;
    }
    writeBackoff.backoff();
  }


  @Override
  public final boolean offer(T tuple) {
    stream.offer(tuple);
    tuplesWritten++;
    return remainingCapacity() > 0;
  }

  @Override
  public T getNextTuple() {
    T tuple = poll();
    if (tuple != null) {
      readBackoff.relax();
      return tuple;
    }
    readBackoff.backoff();
    return null;
  }

  @Override
  public final T poll() {
    T tuple = stream.poll();
    if (tuple != null) {
      tupleRead++;
    }
    return tuple;
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
    return (int) (tuplesWritten - tupleRead);
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
        .append("source", source)
        .append("destination", destination)
        .append("enabled", enabled)
        .appendSuper(super.toString())
        .toString();
  }

  private static enum Factory implements StreamFactory {
    INSTANCE;

    private final AtomicInteger indexes = new AtomicInteger();

    @Override
    public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
        int capacity) {
      return new UnboundedStream<>(getStreamId(from, to), indexes.getAndIncrement(), from, to,
          capacity);
    }
  }
}
