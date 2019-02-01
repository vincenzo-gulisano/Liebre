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
import component.ComponentState;
import component.ComponentType;
import component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import component.operator.Operator;
import stream.Stream;

/**
 * Base abstract {@link Operator2In} implementation.
 *
 * @param <IN> The type of the tuples in the first input.
 * @param <IN2> The type of the tuples in the second input.
 * @param <OUT> The type of the tuples in the output.
 */
public abstract class BaseOperator2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
    implements Operator2In<IN, IN2, OUT> {

  private final ComponentState<Tuple, OUT> state;
  private final Operator<IN2, OUT> secondInputView;

  private final int INPUT1_KEY = 0;
  private final int INPUT2_KEY = 1;
  private final int OUTPUT_KEY = 0;

  private final ProcessCommand2In<IN, IN2, OUT> processCommand = new ProcessCommand2In<>(this);

  /**
   * Construct.
   *
   * @param id The unique ID of this component.operator.
   */
  public BaseOperator2In(String id) {
    this.state = new ComponentState<>(id, ComponentType.OPERATOR2IN);
    this.secondInputView = new SecondInputOperator2InAdapter<>(this);
  }

  @Override
  public ComponentType getType() {
    return state.getType();
  }

  @Override
  public Stream<IN> getInput() {
    return (Stream<IN>) state.getInput(INPUT1_KEY);
  }

  @Override
  public Stream<IN2> getInput2() {
    return (Stream<IN2>) state.getInput(INPUT2_KEY);
  }

  @Override
  public void addInput(StreamProducer<IN> source, Stream<IN> stream) {
    state.addInput(INPUT1_KEY, (Stream<Tuple>) stream);
  }

  @Override
  public void addInput2(StreamProducer<IN2> source, Stream<IN2> stream) {
    state.addInput(INPUT2_KEY, (Stream<Tuple>) stream);
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    state.addOutput(OUTPUT_KEY, stream);
  }

  @Override
  public Stream<OUT> getOutput() {
    return state.getOutput();
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return state.getOutputs();
  }

  @Override
  public Collection<? extends Stream<Tuple>> getInputs() {
    return state.getInputs();
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
  public void runFor(int times) {
    processCommand.runFor(times);
  }

  @Override
  public boolean canRun() {
    return getInput().size() > 0 && getInput2().size() > 0 && getOutput().remainingCapacity() > 0;
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
  public Operator<IN2, OUT> secondInputView() {
    return secondInputView;
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
