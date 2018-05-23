/*  Copyright (C) 2017  Vincenzo Gulisano
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package operator.in2;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ComponentState;
import common.component.ComponentType;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.Objects;
import operator.Operator;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public abstract class BaseOperator2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
    implements Operator2In<IN, IN2, OUT> {

  private final ComponentState<Tuple, OUT> state;

  private final int INPUT1_KEY = 0;
  private final int INPUT2_KEY = 1;

  private final ProcessCommand2In<IN, IN2, OUT> processCommand = new ProcessCommand2In<>(this);

  public BaseOperator2In(String id) {
    state = new ComponentState<>(id, ComponentType.OPERATOR2IN);
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
    state.addOutput(stream);
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
    return new SecondInputOperator2InAdapter<>(this);
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
  public boolean hasInput() {
    return state.hasInput();
  }

  @Override
  public boolean hasOutput() {
    return state.hasOutput();
  }

  @Override
  public void onScheduled() {
  }

  public void onRun() {
  }

  @Override
  public void setPriorityMetric(PriorityMetric metric) {
    processCommand.setMetric(metric);
  }

  @Override
  public String toString() {
    return getId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(state);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof BaseOperator2In)) {
      // Give the other object a chance to check equality. Useful in the case of
      // adapters
      return obj.equals(this);
    }
    BaseOperator2In<?, ?, ?> other = (BaseOperator2In<?, ?, ?>) obj;
    return Objects.equals(state, other.state);
  }

}
