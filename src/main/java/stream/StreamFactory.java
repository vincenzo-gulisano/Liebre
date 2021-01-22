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

import component.StreamConsumer;
import component.StreamProducer;
import common.util.backoff.Backoff;
import common.util.backoff.InactiveBackoff;
import java.util.List;

/** Factory for {@link Stream}s. */
public interface StreamFactory {

  <T> Stream<T> newStream(
      StreamProducer<T> from, StreamConsumer<T> to, int capacity, Backoff backoff);

  default <T> Stream<T> newStream(StreamProducer<T> from, StreamConsumer<T> to, int capacity) {
    return newStream(from, to, capacity, InactiveBackoff.INSTANCE);
  }

  <T extends Comparable<? super T>> MWMRStream<T> newMWMRStream(
          List<? extends StreamProducer<T>> sources, List<? extends StreamConsumer<T>> destinations, int maxLevels, Backoff backoff);

  default String getStreamId(StreamProducer<?> from, StreamConsumer<?> to) {
    return String.format("%s_%s", from.getId(), to.getId());
  }

}
