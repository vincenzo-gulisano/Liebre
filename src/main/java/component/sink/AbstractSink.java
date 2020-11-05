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

package component.sink;

import component.AbstractComponent;
import component.ComponentType;

import java.util.Collection;
import query.LiebreContext;
import stream.Stream;

/**
 * Abstract implementation of a {@link Sink}, controlling basic changes to the state.
 *
 * @param <IN> The type of input tuples
 */
public abstract class AbstractSink<IN> extends AbstractComponent<IN, Void> implements Sink<IN> {

  private static final int INPUT_KEY = 0;

  /**
   * Construct.
   *
   * @param id The unique ID of this component.
   */
  public AbstractSink(String id) {
    super(id, ComponentType.SINK);
  }

  @Override
  protected final void process() {
    if (isFlushed()) {
      return;
    }

    Stream<IN> input = getInput();
    IN tuple = input.getNextTuple(getIndex());

    if (isStreamFinished(tuple, input)) {
      flush();
      return;
    }

    if (tuple != null) {
      increaseTuplesRead();
      increaseTuplesWritten();
      processTuple(tuple);
    }
  }

  @Override
  protected void flushAction() {
    LiebreContext.sinkFinished(this);
  }

  @Override
  public void addInput(Stream<IN> stream) {
    state.addInput(INPUT_KEY, stream);
  }

  @Override
  public Stream<IN> getInput() {
    return state.getInput(INPUT_KEY);
  }

  @Override
  public Collection<? extends Stream<IN>> getInputs() {
    return state.getInputs();
  }

  @Override
  public boolean canRun() {
    return getInput().size() > 0;
  }

  public abstract void processTuple(IN tuple);
}
