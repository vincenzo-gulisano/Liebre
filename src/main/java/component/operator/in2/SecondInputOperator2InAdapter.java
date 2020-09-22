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

import component.ComponentType;
import component.ConnectionsNumber;
import component.operator.Operator;
import java.util.Collection;
import java.util.List;
import stream.Stream;


/**
 * An adapter of this component.operator that looks like a regular {@link
 * component.operator.in1.Operator1In} with its
 * main input being the second input of this {@link Operator2In}.
 */
class SecondInputOperator2InAdapter<IN, OUT> implements
    Operator<IN, OUT> {

  private final Operator2In<?, IN, OUT> decorated;

  /**
   * Construct an adapter for the given component.operator.
   *
   * @param operator The component.operator to be adapted.
   */
  public SecondInputOperator2InAdapter(Operator2In<?, IN, OUT> operator) {
    this.decorated = operator;
  }

  public List<OUT> processTupleIn1(IN tuple) {
    return decorated.processTupleIn2(tuple);
  }

  @Override
  public void addInput(Stream<IN> stream) {
    decorated.addInput2(stream);
  }

  @Override
  public double getRate() {
    return decorated.getRate();
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public Collection<? extends Stream<?>> getInputs() {
    return decorated.getInputs();
  }

  @Override
  public Stream<IN> getInput() {
    return decorated.getInput2();
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
  public ComponentType getType() {
    return decorated.getType();
  }

  @Override
  public double getCost() {
    return decorated.getCost();
  }

  @Override
  public double getSelectivity() {
    return decorated.getSelectivity();
  }

  @Override
  public void updateMetrics() {
    decorated.updateMetrics();
  }

  @Override
  public boolean runFor(int times) {
    return decorated.runFor(times);
  }

  @Override
  public boolean canRun() {
    return decorated.canRun();
  }

  @Override
  public void run() {
    decorated.run();
  }

  @Override
  public void addOutput(Stream<OUT> stream) {
    decorated.addOutput(stream);
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
  public Stream<OUT> getOutput() {
    return decorated.getOutput();
  }

}
