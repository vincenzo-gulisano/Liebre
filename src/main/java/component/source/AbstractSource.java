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
import component.StreamConsumer;
import java.util.Collection;
import stream.Stream;

public abstract class AbstractSource<OUT> extends component.AbstractComponent<Void, OUT>
    implements Source<OUT> {

  private int priority;
  private static final int OUTPUT_KEY = 0;
  private int relativeProducerIndex;

  public AbstractSource(String id) {
    super(id, ComponentType.SOURCE);
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    state.addOutput(OUTPUT_KEY, stream);
  }

  @Override
  public Stream<OUT> getOutput() {
    return state.getOutput(OUTPUT_KEY);
  }

  @Override
  protected final void process() {
    OUT tuple = getNextTuple();
    Stream<OUT> output = getOutput();
    if (tuple != null) {
      increaseTuplesRead();
      increaseTuplesWritten();
      output.addTuple(tuple, getRelativeProducerIndex());
    }
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return state.getOutputs();
  }

  public boolean canRun() {
    return getOutput().remainingCapacity() > 0;
  }

  @Override
  public int getRelativeProducerIndex() {
    return relativeProducerIndex;
  }

  @Override
  public void setRelativeProducerIndex(int index) {
    this.relativeProducerIndex = index;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }
}
