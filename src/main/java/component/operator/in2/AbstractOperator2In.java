package component.operator.in2;

import component.AbstractComponent;
import component.ComponentType;
import component.StreamConsumer;
import component.StreamProducer;
import component.operator.Operator;
import java.util.Collection;
import stream.Stream;

public abstract class AbstractOperator2In<IN, IN2, OUT> extends AbstractComponent<Object, OUT>
    implements Operator2In<IN, IN2, OUT> {

  protected final Operator<IN2, OUT> secondInputView;
  private final int INPUT1_KEY = 0;
  private final int INPUT2_KEY = 1;
  private final int OUTPUT_KEY = 0;

  public AbstractOperator2In(String id, ComponentType type) {
    super(id, type);
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
  public Operator<IN2, OUT> secondInputView() {
    return secondInputView;
  }

  @Override
  public boolean canRun() {
    return getInput().size() > 0 && getInput2().size() > 0 && getOutput().remainingCapacity() > 0;
  }
}
