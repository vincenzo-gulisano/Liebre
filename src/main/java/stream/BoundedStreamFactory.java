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
import common.component.Component;
import common.tuple.Tuple;
import java.util.concurrent.atomic.AtomicInteger;

public enum BoundedStreamFactory implements StreamFactory {
  INSTANCE;

  private static final AtomicInteger indexes = new AtomicInteger();

  @Override
  public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to,
      int capacity) {
    return new BoundedStream<>(getStreamId(from, to), indexes.getAndIncrement(), from, to,
        capacity);
  }

  private String getStreamId(Component from, Component to) {
    return String.format("%s_%s", from.getId(), to.getId());
  }

}
