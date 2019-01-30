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

package component.operator.union;

import component.StreamProducer;
import component.ComponentType;
import common.tuple.Tuple;
import component.operator.AbstractOperator;
import stream.Stream;

/**
 * Operator that unites multiple input streams into one. No guarantee on the ordering of the output
 * tuples.
 *
 * @param <T> The type of input/output tuples.
 */
public class UnionOperator<T extends Tuple> extends AbstractOperator<T, T> {

  /**
   * Construct.
   *
   * @param id The unique ID of the component.operator.
   */
  public UnionOperator(String id) {
    super(id, ComponentType.UNION);
  }

  @Override
  public void addInput(StreamProducer<T> source, Stream<T> stream) {
    state.addInput(stream);
  }

  /**
   * Not meaningful in this component.operator, use {@link #getInputs()} instead.
   *
   * @throws UnsupportedOperationException always, since {@link UnionOperator} has multiple inputs.
   */
  @Override
  public Stream<T> getInput() {
    throw new UnsupportedOperationException(
        String.format("'%s': Unions have multiple inputs!", state.getId()));
  }

  @Override
  public boolean canRun() {
    return getOutput().remainingCapacity() > 0;
  }

  @Override
  public void run() {
    if (isEnabled()) {
      process();
    }
  }

  // TODO: Convert to command like the other operators
  public final void process() {
    Stream<T> output = getOutput();
    for (Stream<T> in : state.getInputs()) {
      T inTuple = in.getNextTuple();
      if (inTuple != null) {
        output.addTuple(inTuple);
      }
    }
  }

}
