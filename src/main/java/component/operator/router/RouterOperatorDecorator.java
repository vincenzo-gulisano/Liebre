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

import component.StreamConsumer;
import component.StreamProducer;
import component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import stream.Stream;

/**
 * Base decorator for {@link RouterOperator}. Delegates all function calls to the decorated object.
 *
 * @param <T> The type of input/output tuples.
 * @author palivosd
 */
public class RouterOperatorDecorator<T extends Tuple> implements RouterOperator<T> {

  private final RouterOperator<T> decorated;
  private final ProcessCommandRouter<T> processingCommand = new ProcessCommandRouter<>(this);

  /**
   * Construct a decorator for the given object.
   *
   * @param decorated The object to be decorated.
   */
  public RouterOperatorDecorator(RouterOperator<T> decorated) {
    this.decorated = decorated;
  }

  @Override
  public String getId() {
    return decorated.getId();
  }

  @Override
  public int getIndex() {
    return decorated.getIndex();
  }

  @Override
  public Collection<? extends Stream<T>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public Collection<? extends Stream<T>> getInputs() {
    return decorated.getInputs();
  }

  @Override
  public void addInput(StreamProducer<T> source, Stream<T> stream) {
    decorated.addInput(source, stream);
  }

  @Override
  public Stream<T> getInput() {
    return decorated.getInput();
  }

  @Override
  public void addOutput(StreamConsumer<T> destination, Stream<T> stream) {
    decorated.addOutput(destination, stream);
  }

  @Override
  public Stream<T> getOutput() {
    return decorated.getOutput();
  }

  @Override
  public void run() {
    processingCommand.run();
  }

  @Override
  public Collection<? extends Stream<T>> chooseOutputs(T tuple) {
    return decorated.chooseOutputs(tuple);
  }

  @Override
  public String toString() {
    return decorated.toString();
  }

  @Override
  public void enable() {
    decorated.enable();
  }

  @Override
  public boolean isEnabled() {
    return decorated.isEnabled();
  }

  @Override
  public void disable() {
    decorated.disable();
  }

  @Override
  public ConnectionsNumber inputsNumber() {
    return decorated.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return decorated.outputsNumber();
  }

  @Override
  public void runFor(int times) {
    decorated.runFor(times);
  }

  @Override
  public boolean canRun() {
    return decorated.canRun();
  }
}
