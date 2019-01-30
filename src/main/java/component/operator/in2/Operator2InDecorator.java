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

package component.operator.in2;

import component.StreamConsumer;
import component.StreamProducer;
import component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import component.operator.Operator;
import stream.Stream;

/**
 * Base decorator for {@link Operator2In}. Delegates all function calls to the decorated object.
 *
 * @param <IN> The type of input tuples in the first input stream.
 * @param <IN2> The type of input tuples in the second input stream.
 * @param <OUT> The type of output tuples.
 * @author palivosd
 */
public class Operator2InDecorator<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
    implements Operator2In<IN, IN2, OUT> {

  private final Operator2In<IN, IN2, OUT> decorated;
  private final ProcessCommand2In<IN, IN2, OUT> processingCommand = new ProcessCommand2In<>(this);

  /**
   * Create a decorator for the given component.operator.
   *
   * @param decorated The component.operator to be decorated.
   */
  public Operator2InDecorator(Operator2In<IN, IN2, OUT> decorated) {
    this.decorated = decorated;
  }

  @Override
  public List<OUT> processTupleIn1(IN tuple) {
    return decorated.processTupleIn1(tuple);
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
  public void run() {
    processingCommand.run();
  }

  @Override
  public Collection<? extends Stream<Tuple>> getInputs() {
    return decorated.getInputs();
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
  public void addInput2(StreamProducer<IN2> source, Stream<IN2> stream) {
    decorated.addInput2(source, stream);
  }

  @Override
  public boolean canRun() {
    return decorated.canRun();
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public Stream<IN2> getInput2() {
    return decorated.getInput2();
  }

  @Override
  public List<OUT> processTupleIn2(IN2 tuple) {
    return decorated.processTupleIn2(tuple);
  }


  @Override
  public Operator<IN2, OUT> secondInputView() {
    return decorated.secondInputView();
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
  public String toString() {
    return decorated.toString();
  }

}
