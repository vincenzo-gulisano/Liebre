package sink;

import common.StreamProducer;
import common.component.ComponentState;
import common.component.ComponentType;
import common.component.ConnectionsNumber;
import common.component.EventType;
import common.tuple.Tuple;
import java.util.Collection;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public abstract class AbstractSink<IN extends Tuple> implements Sink<IN> {

  private static final int INPUT_KEY = 0;
  protected final ComponentState<IN, Tuple> state;
  private final ProcessCommandSink<IN> processCommand = new ProcessCommandSink<>(this);

  public AbstractSink(String id) {
    state = new ComponentState<>(id, ComponentType.SINK);
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
  public Collection<? extends Stream<IN>> getInputs() {
    return state.getInputs();
  }

  @Override
  public boolean canRead() {
    return state.canRead();
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
  public String getId() {
    return state.getId();
  }

  @Override
  public int getIndex() {
    return state.getIndex();
  }

  public abstract void processTuple(IN tuple);

  @Override
  public void setPriorityMetric(PriorityMetric metric) {
    processCommand.setMetric(metric);
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
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((state == null) ? 0 : state.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof BaseSink)) {
      return false;
    }
    AbstractSink<? extends Tuple> other = (AbstractSink<? extends Tuple>) obj;
    if (state == null) {
      if (other.state != null) {
        return false;
      }
    } else if (!state.equals(other.state)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getId();
  }
}
