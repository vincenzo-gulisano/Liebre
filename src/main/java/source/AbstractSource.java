package source;

import common.StreamConsumer;
import common.component.ComponentState;
import common.component.ComponentType;
import common.component.ConnectionsNumber;
import common.component.EventType;
import common.tuple.Tuple;
import java.util.Collection;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public abstract class AbstractSource<OUT extends Tuple> implements Source<OUT> {

  private static final int OUTPUT_KEY = 0;
  protected final ComponentState<Tuple, OUT> state;
  private final ProcessCommandSource<OUT> processCommand = new ProcessCommandSource<>(this);

  public AbstractSource(String id) {
    this.state = new ComponentState<>(id, ComponentType.SOURCE);
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    state.addOutput(OUTPUT_KEY, stream);
  }

  @Override
  public Stream<OUT> getOutput() {
    return state.getOutput(OUTPUT_KEY);
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return state.getOutputs();
  }

  @Override
  public boolean canWrite() {
    return state.canWrite();
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
  public ConnectionsNumber inputsNumber() {
    return state.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return state.outputsNumber();
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
  public abstract OUT getNextTuple();

  @Override
  public String getId() {
    return state.getId();
  }

  @Override
  public int getIndex() {
    return state.getIndex();
  }

  @Override
  public void onScheduled() {
  }

  @Override
  public void onRun() {
  }

  @Override
  public void setPriorityMetric(PriorityMetric metric) {
    processCommand.setMetric(metric);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractSource<?> that = (AbstractSource<?>) o;

    return new EqualsBuilder()
        .append(state, that.state)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(state)
        .toHashCode();
  }
}
