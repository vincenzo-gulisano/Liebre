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

package operator.in2;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import operator.Operator;
import stream.Stream;

class SecondInputOperator2InAdapter<IN extends Tuple, OUT extends Tuple> implements
    Operator<IN, OUT> {

  private final Operator2In<?, IN, OUT> decorated;

  public SecondInputOperator2InAdapter(Operator2In<?, IN, OUT> operator) {
    this.decorated = operator;
  }

  public List<OUT> processTupleIn1(IN tuple) {
    return decorated.processTupleIn2(tuple);
  }

  @Override
  public void addInput(StreamProducer<IN> source, Stream<IN> stream) {
    decorated.addInput2(source, stream);
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public Collection<? extends Stream<Tuple>> getInputs() {
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
  public void onScheduled() {
    decorated.onScheduled();
  }

  @Override
  public void onRun() {
    decorated.onRun();
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
    decorated.run();
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    decorated.addOutput(destination, stream);
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
