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
import common.component.EventType;
import common.tuple.Tuple;
import common.util.backoff.BackoffFactory;

public class NotifyingStream<T extends Tuple> extends StreamDecorator<T> {

  public NotifyingStream(Stream<T> decorated) {
    super(decorated);
  }

  @Override
  public void addTuple(T tuple) {
    if (!offer(tuple)) {
      getSource().waitFor(EventType.READ);
    }
    getDestination().notifyFor(EventType.WRITE);
  }


  @Override
  public T getNextTuple() {
    T tuple = poll();
    if (tuple == null) {
      getDestination().waitFor(EventType.WRITE);
    }
    getSource().notifyFor(EventType.READ);
    return tuple;
  }

  public static StreamFactory factory() {
    return Factory.INSTANCE;
  }

  private static enum Factory implements StreamFactory {
    INSTANCE;

    @Override
    public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
        int capacity, BackoffFactory backoff) {
      return new NotifyingStream<>(UnboundedStream.factory().newStream(from, to, capacity));
    }

  }
}
