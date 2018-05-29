package sink;

import common.StreamProducer;
import common.component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class SinkDecorator<IN extends Tuple> implements Sink<IN> {

  private final Sink<IN> decorated;
  private final ProcessCommandSink<IN> processCommand = new ProcessCommandSink<>(this);

  public SinkDecorator(Sink<IN> decorated) {
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
  public boolean hasInput() {
    return decorated.hasInput();
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
  public Collection<? extends Stream<IN>> getInputs() {
    return decorated.getInputs();
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
  public void processTuple(IN tuple) {
    decorated.processTuple(tuple);
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
  public String toString() {
    return decorated.toString();
  }

  public void setPriorityMetric(PriorityMetric metric) {
    decorated.setPriorityMetric(metric);
  }

}
