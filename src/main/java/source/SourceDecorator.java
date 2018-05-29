package source;

import common.StreamConsumer;
import common.component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class SourceDecorator<OUT extends Tuple> implements Source<OUT> {

  private final Source<OUT> decorated;
  private final ProcessCommandSource<OUT> processCommand = new ProcessCommandSource<>(this);

  public SourceDecorator(Source<OUT> decorated) {
    this.decorated = decorated;
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
    processCommand.run();
  }

  @Override
  public boolean hasOutput() {
    return decorated.hasOutput();
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
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
  public String getId() {
    return decorated.getId();
  }

  @Override
  public int getIndex() {
    return decorated.getIndex();
  }

  @Override
  public OUT getNextTuple() {
    return decorated.getNextTuple();
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

  @Override
  public void setPriorityMetric(PriorityMetric metric) {
    decorated.setPriorityMetric(metric);
  }

  @Override
  public String toString() {
    return decorated.toString();
  }

}
