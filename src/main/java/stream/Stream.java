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

import io.palyvos.dcs.common.Active;
import io.palyvos.dcs.common.Named;
import component.StreamConsumer;
import component.StreamProducer;
import java.util.List;

/**
 * An ordered one-on-one stream that connects exactly one {@link StreamProducer} and one {@link
 * StreamConsumer}.
 *
 * @param <T> The type of values that can be transfered inside the stream
 */
public interface Stream<T> extends Active, Named {

  void addTuple(T tuple,int writer);

  boolean offer(T tuple,int writer);

  T getNextTuple(int reader);

  T peek(int reader);

  int remainingCapacity();

  int size();

  List<StreamProducer<T>> getSources();

  List<StreamConsumer<T>> getDestinations();

  void resetArrivalTime();

  double getAverageArrivalTime();

}
