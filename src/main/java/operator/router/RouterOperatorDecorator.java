package operator.router;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class RouterOperatorDecorator<T extends Tuple> implements RouterOperator<T> {

	private final RouterOperator<T> decorated;
	private final ProcessCommandRouter<T> processingCommand = new ProcessCommandRouter<>(this);

	public RouterOperatorDecorator(RouterOperator<T> decorated) {
		this.decorated = decorated;
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
	public Collection<? extends Stream<T>> getOutputs() {
		return decorated.getOutputs();
	}

  @Override
  public Collection<? extends Stream<T>> getInputs() {
    return decorated.getInputs();
  }

  @Override
	public boolean hasInput() {
		return decorated.hasInput();
	}

	@Override
	public void addInput(StreamProducer<T> source, Stream<T> stream) {
		decorated.addInput(source, stream);
	}

	@Override
	public Stream<T> getInput() {
		return decorated.getInput();
	}

	@Override
	public boolean hasOutput() {
		return decorated.hasOutput();
	}

	@Override
	public void addOutput(StreamConsumer<T> destination, Stream<T> stream) {
		decorated.addOutput(destination, stream);
	}

	@Override
	public Stream<T> getOutput() {
		return decorated.getOutput();
	}

	@Override
	public void run() {
		processingCommand.run();
	}

	@Override
	public List<String> chooseOperators(T tuple) {
		return decorated.chooseOperators(tuple);
	}

	@Override
	public String toString() {
		return decorated.toString();
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
