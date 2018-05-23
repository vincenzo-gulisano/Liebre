package operator.in2;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import operator.Operator;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class Operator2InDecorator<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		implements Operator2In<IN, IN2, OUT> {

	private final Operator2In<IN, IN2, OUT> decorated;
	private final ProcessCommand2In<IN, IN2, OUT> processingCommand = new ProcessCommand2In<>(this);

	public Operator2InDecorator(Operator2In<IN, IN2, OUT> decorated) {
		this.decorated = decorated;
	}

	@Override
	public List<OUT> processTupleIn1(IN tuple) {
		return decorated.processTupleIn1(tuple);
	}

	@Override
	public boolean hasInput() {
		return decorated.hasInput();
	}

	@Override
	public boolean hasOutput() {
		return decorated.hasOutput();
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
	public void run() {
		processingCommand.run();
	}

  @Override
  public Collection<? extends Stream<Tuple>> getInputs() {
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
	public void addInput2(StreamProducer<IN2> source, Stream<IN2> stream) {
		decorated.addInput2(source, stream);
	}

	@Override
	public Collection<? extends Stream<OUT>> getOutputs() {
		return decorated.getOutputs();
	}

	@Override
	public Stream<IN2> getInput2() {
		return decorated.getInput2();
	}

	@Override
	public List<OUT> processTupleIn2(IN2 tuple) {
		return decorated.processTupleIn2(tuple);
	}



	@Override
	public Operator<IN2, OUT> secondInputView() {
		return decorated.secondInputView();
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

	public void setPriorityMetric(PriorityMetric metric) {
		decorated.setPriorityMetric(metric);
	}

	@Override
	public String toString() {
		return decorated.toString();
	}

}
