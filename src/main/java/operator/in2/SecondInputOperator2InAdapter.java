package operator.in2;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ConnectionsNumber;
import common.component.EventType;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import operator.Operator;
import scheduling.priority.PriorityMetric;
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
  public boolean canRead() {
    return decorated.canRead();
  }

  @Override
  public void wait(EventType type) {
    decorated.wait(type);
  }

  @Override
  public void notify(EventType type) {
    decorated.notify(type);
  }

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
  public void setPriorityMetric(PriorityMetric metric) {
    decorated.setPriorityMetric(metric);
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
  public boolean canWrite() {
    return decorated.canWrite();
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
