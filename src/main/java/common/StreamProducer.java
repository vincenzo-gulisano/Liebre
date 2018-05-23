/*  Copyright (C) 2017-2018  Vincenzo Gulisano, Dimitris Palyvos Giannas
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact:
 *    Vincenzo Gulisano info@vincenzogulisano.com
 *    Dimitris Palyvos Giannas palyvos@chalmers.se
 */

package common;

import common.component.Component;
import common.tuple.Tuple;
import java.util.Collection;
import operator.in2.Operator2In;
import stream.Stream;

/**
 * A stream {@link Component} that produces tuples.
 *
 * @param <OUT> The output type of this component.
 */
public interface StreamProducer<OUT extends Tuple> extends Named, Component {

  /**
   * Connect this component to a downstream {@link StreamConsumer}.
   * @param out The {@link StreamConsumer} to connect this component to.
   */
  void addOutput(StreamConsumer<OUT> out);

  /**
   * FIXME: Some way to avoid this
   * For internal use only
   * @param out
   */
  default void addOutput(Operator2In<?, OUT, ?> out) {
    addOutput(out.secondInputView());
  }

  /**
   * Get all the downstream {@link StreamConsumer}s that are directly connected to this component.
   * @return A collection of components that are read the output of this component.
   */
  Collection<StreamConsumer<OUT>> getNext();

  /**
   * Get the output {@link Stream} of this {@link StreamProducer}. If the instance has multiple
   * output {@link Stream}s, then the stream connected to the given entity id is returned.
   *
   * @param requestorId The unique ID of the {@link StreamConsumer} that is connected to this input
   * stream. This is only used in cases where the operator has more than one output streams and we
   * need to know both ends of the stream to return it correctly.
   * @return The output {@link Stream} of this {@link StreamProducer}.
   */
  Stream<OUT> getOutputStream(String requestorId);

  /**
   * Heuristic that indicates if the {@link StreamProducer} can write tuples to all its output
   * streams.
   *
   * @return {@code true} if the operator can write tuples to all its output streams
   */
  boolean hasOutput();

}
