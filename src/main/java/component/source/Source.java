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

package component.source;

import component.StreamProducer;
import common.tuple.Tuple;

/**
 * The component.source is the first component in a query. It generates the tuples that are
 * processed by the
 * stream processing system.
 *
 * @param <OUT> The type of output tuples.
 */
public interface Source<OUT extends Tuple> extends StreamProducer<OUT> {

  OUT getNextTuple();

  @Override
  default int getTopologicalOrder() {
    return 1;
  }

  @Override
  default double getHeadArrivalTime() {
    return -1;
  }

  @Override
  default double getAverageArrivalTime() {
    return -1;
  }
}
