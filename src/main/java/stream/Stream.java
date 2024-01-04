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
import common.Active;
import common.Named;
import java.util.List;

/**
 * An ordered one-on-one stream that connects exactly one {@link StreamProducer} and one {@link
 * StreamConsumer}.
 *
 * @param <T> The type of values that can be transferred inside the stream
 */
public interface Stream<T> extends Active, Named {

  void addTuple(T tuple, int producerIndex);

  boolean offer(T tuple, int producerIndex);

  T getNextTuple(int consumerIndex);

  T peek(int consumerIndex);

  int remainingCapacity();

  int size();

  List<? extends StreamProducer<T>> producers();

  List<? extends StreamConsumer<T>> consumers();

  void resetArrivalTime();

  double averageArrivalTime();

  void flush();

  void clear();

  boolean isFlushed();

}
