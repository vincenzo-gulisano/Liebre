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

package component;

import io.palyvos.dcs.common.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import stream.Stream;

/**
 * A stream {@link Component} that produces tuples.
 *
 * @param <OUT> The output type of this component.
 */
public interface StreamProducer<OUT> extends Named, Component {

  /**
   * Connect this producer with the given {@link StreamConsumer} using the provided stream.
   * Different implementations allow one or more calls to this function.
   *
   * @param destination The consumer fed by this consumer.
   * @param stream The {@link Stream} that forms the data connection.
   * @see ConnectionsNumber
   */
  void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream);

  /**
   * Get the output {@link Stream} of this producer, <emph>if is the type of producer that always
   * has a unique input.</emph> {@link StreamProducer}s that cannot conform to this interface can
   * throw {@link UnsupportedOperationException} (this is done for example in {@link
   * component.operator.router.BaseRouterOperator}.
   *
   * @return The unique output stream of this producer.
   */
  Stream<OUT> getOutput() throws UnsupportedOperationException;

  /**
   * Get all the output {@link Stream}s of this producer.
   *
   * @return All the output streams of this producer.
   */
  Collection<? extends Stream<OUT>> getOutputs();

  @Override
  default List<Component> getDownstream() {
    List<Component> downstream = new ArrayList<>();
    for (Stream<?> output : getOutputs()) {
      for (StreamConsumer<?> op : output.consumers()) {
        downstream.add(op);
      }
    }
    return downstream;
  }

  @Override
  default long getOutputQueueSize() {
    long size = 0;
    for (Stream<?> output : getOutputs()) {
      size += output.size();
    }
    return size;
  }

  /**
   * Get the relative index (starting in 0) of this entity with respect to a connected downstream
   * entity
   *
   * @return The relative index of this entity as producer of its connected downstream entity
   */
  int getRelativeProducerIndex();

  void setRelativeProducerIndex(int index);
}
