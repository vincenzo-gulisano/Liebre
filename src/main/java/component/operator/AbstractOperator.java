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

import component.AbstractComponent;
import component.ComponentType;
import component.StreamConsumer;
import component.StreamProducer;
import java.util.Collection;
import stream.Stream;

/**
 * Abstract implementation of {@link Operator} that handles basic changes to the state of the
 * component.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public abstract class AbstractOperator<IN, OUT> extends AbstractComponent<IN, OUT>
    implements Operator<IN, OUT> {

  private final int INPUT_KEY = 0;
  private final int OUTPUT_KEY = 0;

  public AbstractOperator(
      String id, ComponentType type) {
    super(id, type);
  }

  @Override
  public ComponentType getType() {
    return state.getType();
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    state.addOutput(OUTPUT_KEY, stream);
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
  public Stream<OUT> getOutput() {
    return state.getOutput(OUTPUT_KEY);
  }

  public Collection<? extends Stream<IN>> getInputs() {
    return state.getInputs();
  }

  public Collection<? extends Stream<OUT>> getOutputs() {
    return state.getOutputs();
  }

  @Override
  public boolean canRun() {
    return getInput().size() > 0 && getOutput().remainingCapacity() > 0;
  }

}
