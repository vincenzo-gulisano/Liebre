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

import component.AbstractComponent;
import component.ComponentType;
import component.StreamConsumer;
import component.StreamProducer;
import component.operator.Operator;
import java.util.Collection;
import java.util.List;
import stream.Stream;

/**
 * Base abstract {@link Operator2In} implementation.
 *
 * @param <IN> The type of the tuples in the first input.
 * @param <IN2> The type of the tuples in the second input.
 * @param <OUT> The type of the tuples in the output.
 */
public abstract class BaseOperator2In<IN, IN2, OUT> extends AbstractComponent<Object, OUT>
    implements Operator2In<IN, IN2, OUT> {

  private final Operator<IN2, OUT> secondInputView;
  private int relativeProducerIndex;
  private int relativeConsumerIndex;

  private final int INPUT1_KEY = 0;
  private final int INPUT2_KEY = 1;
  private final int OUTPUT_KEY = 0;

  /**
   * Construct.
   *
   * @param id The unique ID of this component.operator.
   */
  public BaseOperator2In(String id, int relativeProducerIndex, int relativeConsumerIndex) {
    super(id, ComponentType.OPERATOR2IN);
    this.secondInputView = new SecondInputOperator2InAdapter<>(this);
    this.relativeProducerIndex = relativeProducerIndex;
    this.relativeConsumerIndex = relativeConsumerIndex;
  }

  @Override
  protected void process() {
    Stream<IN> input1 = getInput();
    Stream<IN2> input2 = getInput2();
    Stream<OUT> output = getOutput();

    IN inTuple1 = input1.getNextTuple(getRelativeConsumerIndex());
    IN2 inTuple2 = input2.getNextTuple(getRelativeConsumerIndex());
    if (inTuple1 != null) {
      increaseTuplesRead();
      List<OUT> outTuples = processTupleIn1(inTuple1);
      if (outTuples != null) {
        for (OUT t : outTuples) {
          increaseTuplesWritten();
          output.addTuple(t, getRelativeProducerIndex());
        }
      }
    }
    if (inTuple2 != null) {
      increaseTuplesRead();
      List<OUT> outTuples = processTupleIn2(inTuple2);
      if (outTuples != null) {
        for (OUT t : outTuples) {
          increaseTuplesWritten();
          output.addTuple(t, getRelativeProducerIndex());
        }
      }
    }
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
    state.addInput(INPUT1_KEY, (Stream<Object>) stream);
  }

  @Override
  public void addInput2(StreamProducer<IN2> source, Stream<IN2> stream) {
    state.addInput(INPUT2_KEY, (Stream<Object>) stream);
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
  public Collection<? extends Stream<?>> getInputs() {
    return state.getInputs();
  }


  @Override
  public boolean canRun() {
    return getInput().size() > 0 && getInput2().size() > 0 && getOutput().remainingCapacity() > 0;
  }


  @Override
  public Operator<IN2, OUT> secondInputView() {
    return secondInputView;
  }

  public int getRelativeProducerIndex() {
    return relativeProducerIndex;
  }

  @Override
  public void setRelativeProducerIndex(int index) {
    this.relativeProducerIndex = index;
  }

  @Override
  public int getRelativeConsumerIndex() {
    return relativeConsumerIndex;
  }

  @Override
  public void setRelativeConsumerIndex(int index) {
    this.relativeConsumerIndex = index;
  }
}
