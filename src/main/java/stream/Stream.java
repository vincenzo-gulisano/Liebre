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

import common.Active;
import common.StreamConsumer;
import common.StreamProducer;
import common.component.Component;
import common.Named;
import common.tuple.Tuple;

/**
 * A physical "stream" that connects to {@link Component}s
 *
 * @param <T> The type of values that can be transfered inside the stream
 */
public interface Stream<T extends Tuple> extends Active, Named {

  void addTuple(T tuple);

  boolean offer(T tuple);

  T getNextTuple();

  T poll();

  T peek();

  int remainingCapacity();

  int size();

  StreamProducer<T> getSource();

  StreamConsumer<T> getDestination();

}
