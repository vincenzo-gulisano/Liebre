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

package component.operator.router;

import component.ComponentType;
import component.StreamConsumer;
import component.operator.AbstractOperator;
import java.util.Collection;
import stream.Stream;

/**
 * Default implementation for {@link RouterOperator}.
 *
 * @param <T> The type of input/output tuples.
 */
public class BaseRouterOperator<T> extends AbstractOperator<T, T> implements RouterOperator<T> {

  public BaseRouterOperator(String id) {
    super(id, ComponentType.ROUTER);
  }

  @Override
  protected final void process() {
    Stream<T> input = getInput();
    T inTuple = input.getNextTuple(getRelativeConsumerIndex());
    if (inTuple != null) {
      increaseTuplesRead();
      for (Stream<T> output : chooseOutputs(inTuple)) {
        increaseTuplesWritten();
        output.addTuple(inTuple, getRelativeProducerIndex());
      }
    }
  }

  @Override
  public Collection<? extends Stream<T>> chooseOutputs(T tuple) {
    return getOutputs();
  }

  @Override
  public void addOutput(StreamConsumer<T> destination, Stream<T> stream) {
    state.addOutput(stream);
  }

  public Stream<T> getOutput() {
    throw new UnsupportedOperationException(
        String.format("'%s': Router has multiple outputs!", state.getId()));
  }

  @Override
  public boolean canRun() {
    if (getInput().size() == 0) {
      return false;
    }
    for (Stream<?> output : getOutputs()) {
      if (output.remainingCapacity() > 0) {
        return true;
      }
    }
    return false;
  }
}
