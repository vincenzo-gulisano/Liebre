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

package component.operator;

import component.StreamConsumer;
import component.StreamProducer;
import component.ComponentState;
import component.ComponentType;
import component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import stream.SSSRStream;

/**
 * Abstract implementation of {@link Operator} that handles basic changes to the state of the
 * component.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public abstract class AbstractOperator<IN extends Tuple, OUT extends Tuple> implements
    Operator<IN, OUT> {

  protected final ComponentState<IN, OUT> state;
  private final int INPUT_KEY = 0;
  private final int OUTPUT_KEY = 0;

  public AbstractOperator(String id, ComponentType type) {
    state = new ComponentState<>(id, type);
  }

  @Override
  public ComponentType getType() {
    return state.getType();
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, SSSRStream<OUT> stream) {
    state.addOutput(OUTPUT_KEY, stream);
  }

  @Override
  public void addInput(StreamProducer<IN> source, SSSRStream<IN> stream) {
    state.addInput(INPUT_KEY, stream);
  }

  @Override
  public SSSRStream<IN> getInput() {
    return state.getInput(INPUT_KEY);
  }

  @Override
  public SSSRStream<OUT> getOutput() {
    return state.getOutput(OUTPUT_KEY);
  }

  public Collection<? extends SSSRStream<IN>> getInputs() {
    return state.getInputs();
  }

  public Collection<? extends SSSRStream<OUT>> getOutputs() {
    return state.getOutputs();
  }

  @Override
  public boolean canRun() {
    return getInput().size() > 0 && getOutput().remainingCapacity() > 0;
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

  @Override
  public String toString() {
    return getId();
  }

  @Override
  public ConnectionsNumber inputsNumber() {
    return state.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return state.outputsNumber();
  }


}