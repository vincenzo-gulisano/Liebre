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

import common.Named;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import scheduling.toolkit.Task;
import stream.Stream;

/**
 * A stream {@link Component} that consumes tuples.
 *
 * @param <IN> The input type for this component.
 */
public interface StreamConsumer<IN extends Tuple> extends Named, Component {

  /**
   * Connect this consumer with the given {@link StreamProducer} using the provided stream.
   * Different implementations allow one or more calls to this function.
   *
   * @param source The producer feeding this consumer.
   * @param stream The {@link Stream} that forms the data connection.
   * @see ConnectionsNumber
   */
  void addInput(StreamProducer<IN> source, Stream<IN> stream);

  /**
   * Get the input {@link Stream} of this consumer, if is the type of consumer that always has a
   * unique input. {@link StreamConsumer}s that cannot conform to this interface can throw {@link
   * UnsupportedOperationException} (this is done for example in {@link
   * component.operator.union.UnionOperator})
   *
   * @return The unique input stream of this consumer.
   */
  Stream<IN> getInput() throws UnsupportedOperationException;

  /**
   * Get all the input {@link Stream}s of this consumer.
   *
   * @param <T> The superclass of all input contents (in the case of input streams of different
   * types, as in {@link component.operator.in2.Operator2In}.
   * @return All the input streams of this consumer.
   */
  <T extends Tuple> Collection<? extends Stream<T>> getInputs();

  @Override
  default int getTopologicalOrder() {
    for (Stream<?> input : getInputs()) {
      int upstreamOrder = input.getSource().getTopologicalOrder();
      return upstreamOrder + 1;
    }
    throw new IllegalStateException("StreamConsumer with no inputs!");
  }

  @Override
  default List<Task> getUpstream() {
    List<Task> upstream = new ArrayList<>();
    for (Stream<?> input : getInputs()) {
     upstream.add(input.getSource());
    }
    return upstream;
  }

  /**
   * Get the latency at the head of the queue of the component.
   * Warning: This will fail if the component is not processing {@link common.tuple.RichTuple}s.
   *
   * @return The head latency of the Component (averaged over all the inputs).
   */
  default double getHeadArrivalTime() {
    Collection<? extends Stream<?>> inputs = getInputs();
    double latencySum = -1;
    for (Stream<?> input : inputs) {
      Object head = input.peek();
      if (head != null) {
        if (head instanceof RichTuple == false) {
          // This stream has no latency info
          continue;
        }
        RichTuple headTuple = (RichTuple) head;
        latencySum += headTuple.getStimulus();
      }
    }
    return latencySum / inputs.size();
  }
}
