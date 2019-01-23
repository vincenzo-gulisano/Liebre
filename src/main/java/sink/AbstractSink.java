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

package sink;

import common.StreamProducer;
import common.component.ComponentState;
import common.component.ComponentType;
import common.component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import stream.Stream;

public abstract class AbstractSink<IN extends Tuple> implements Sink<IN> {

  private static final int INPUT_KEY = 0;
  protected final ComponentState<IN, Tuple> state;
  private final ProcessCommandSink<IN> processCommand = new ProcessCommandSink<>(this);

  public AbstractSink(String id) {
    state = new ComponentState<>(id, ComponentType.SINK);
  }

  @Override
  public void addInput(StreamProducer<IN> source, Stream<IN> stream) {
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
  public void run() {
    processCommand.run();
  }

  @Override
  public void enable() {
    state.enable();
  }

  @Override
  public void disable() {
    state.disable();
  }

  @Override
  public boolean isEnabled() {
    return state.isEnabled();
  }

  @Override
  public String getId() {
    return state.getId();
  }

  @Override
  public int getIndex() {
    return state.getIndex();
  }

  public abstract void processTuple(IN tuple);

  @Override
  public ConnectionsNumber inputsNumber() {
    return state.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return state.outputsNumber();
  }

  @Override
  public String toString() {
    return getId();
  }
}
