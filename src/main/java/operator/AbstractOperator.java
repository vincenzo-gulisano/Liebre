package operator;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ComponentState;
import common.component.ComponentType;
import common.component.ConnectionsNumber;
import common.component.EventType;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.Objects;
import stream.Stream;

public abstract class AbstractOperator<IN extends Tuple, OUT extends Tuple> implements
    Operator<IN, OUT> {

  protected final ComponentState<IN, OUT> state;
  private final int INPUT_KEY = 0;
  private final int OUTPUT_KEY = 0;

  public AbstractOperator(String id, ComponentType type) {
    state = new ComponentState<>(id, type);
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
  public void wait(EventType type) {
    type.wait(state);
  }

  @Override
  public void notify(EventType type) {
    type.notify(state);
  }

  @Override
  public boolean canRead() {
    return state.canRead();
  }

  @Override
  public boolean canWrite() {
    return state.canWrite();
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
  public String getId() {
    return state.getId();
  }

  @Override
  public int getIndex() {
    return state.getIndex();
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
  public void onScheduled() {
  }

  @Override
  public void onRun() {
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof AbstractOperator)) {
      return false;
    }
    AbstractOperator<?, ?> other = (AbstractOperator<?, ?>) obj;
    return Objects.equals(state, other.state);
  }

}