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

import component.ComponentType;

import java.util.Collection;
import query.LiebreContext;
import stream.Stream;

public abstract class AbstractSource<OUT> extends component.AbstractComponent<Void, OUT>
    implements Source<OUT> {

  private int priority;
  private static final int OUTPUT_KEY = 0;

  public AbstractSource(String id) {
    super(id, ComponentType.SOURCE);
  }

  @Override
  public void addOutput(Stream<OUT> stream) {
    state.addOutput(OUTPUT_KEY, stream);
  }

  @Override
  public Stream<OUT> getOutput() {
    return state.getOutput(OUTPUT_KEY);
  }

  @Override
  protected final void process() {

    if (isFlushed()) {
      return;
    }

    OUT tuple = getNextTuple();
    Stream<OUT> output = getOutput();
    if (isInputFinished()) {
      flush();
      return;
    }
    if (tuple != null) {
      increaseTuplesRead();
      increaseTuplesWritten();
      output.addTuple(tuple, getIndex());
    }
  }

  protected abstract boolean isInputFinished();

  @Override
  protected void flushAction() {
    if (LiebreContext.isFlushingEnabled()) {
      getOutput().flush();
    }
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return state.getOutputs();
  }

  public boolean canRun() {
    return getOutput().remainingCapacity() > 0;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }
}
