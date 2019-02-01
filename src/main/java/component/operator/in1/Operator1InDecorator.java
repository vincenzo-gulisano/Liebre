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

package component.operator.in1;

import component.StreamConsumer;
import component.StreamProducer;
import component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import stream.Stream;

/**
 * Base decorator for {@link Operator1In}. Delegates all function calls to the decorated object.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 * @author palivosd
 */
public class Operator1InDecorator<IN extends Tuple, OUT extends Tuple> implements
    Operator1In<IN, OUT> {

  private final Operator1In<IN, OUT> decorated;
  private final ProcessCommand1In<IN, OUT> processCommand = new ProcessCommand1In<>(this);

  /**
   * Create a decorator for the given component.operator.
   *
   * @param decorated The component.operator to be decorated.
   */
  public Operator1InDecorator(Operator1In<IN, OUT> decorated) {
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
  public void addInput(StreamProducer<IN> source, Stream<IN> stream) {
    decorated.addInput(source, stream);
  }

  @Override
  public Stream<IN> getInput() {
    return decorated.getInput();
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    decorated.addOutput(destination, stream);
  }

  @Override
  public Stream<OUT> getOutput() {
    return decorated.getOutput();
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public Collection<? extends Stream<IN>> getInputs() {
    return decorated.getInputs();
  }

  @Override
  public List<OUT> processTupleIn1(IN tuple) {
    return decorated.processTupleIn1(tuple);
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
  public void run() {
    processCommand.run();
  }

  @Override
  public void runFor(int times) {
    processCommand.runFor(times);
  }

  @Override
  public void updateMetrics() {
    processCommand.updateMetrics();
  }

  @Override
  public double getSelectivity() {
    return processCommand.getSelectivity();
  }

  @Override
  public double getCost() {
    return processCommand.getCost();
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
  public boolean canRun() {
    return decorated.canRun();
  }

  @Override
  public String toString() {
    return decorated.toString();
  }
}
