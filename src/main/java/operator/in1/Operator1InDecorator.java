package operator.in1;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ConnectionsNumber;
import common.component.EventType;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class Operator1InDecorator<IN extends Tuple, OUT extends Tuple> implements
    Operator1In<IN, OUT> {

  private final Operator1In<IN, OUT> decorated;
  private final ProcessCommand1In<IN, OUT> processCommand = new ProcessCommand1In<>(this);

  public Operator1InDecorator(Operator1In<IN, OUT> decorated) {
    this.decorated = decorated;
  }

  @Override
  public void run() {
    processCommand.run();
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
  public void addInput(StreamProducer<IN> source, Stream<IN> stream) {
    decorated.addInput(source, stream);
  }

  @Override
  public Stream<IN> getInput() {
    return decorated.getInput();
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
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public Collection<? extends Stream<IN>> getInputs() {
    return decorated.getInputs();
  }

  @Override
  public boolean canRead() {
    return decorated.canRead();
  }

  @Override
  public boolean canWrite() {
    return decorated.canWrite();
  }


  @Override
  public void wait(EventType type) {
    decorated.wait(type);
  }

  @Override
  public void notify(EventType type) {
    decorated.notify(type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Operator1InDecorator<?, ?> that = (Operator1InDecorator<?, ?>) o;

    return new EqualsBuilder()
        .append(decorated, that.decorated)
        .append(processCommand, that.processCommand)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(decorated)
        .append(processCommand)
        .toHashCode();
  }

  @Override
  public String toString() {
    return decorated.toString();
  }

  @Override
  public List<OUT> processTupleIn1(IN tuple) {
    return decorated.processTupleIn1(tuple);
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
  public void onScheduled() {
    decorated.onScheduled();
  }

  @Override
  public void onRun() {
    decorated.onRun();
  }

  @Override
  public ConnectionsNumber inputsNumber() {
    return decorated.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return decorated.outputsNumber();
  }

  public void setPriorityMetric(PriorityMetric metric) {
    decorated.setPriorityMetric(metric);
  }

}
