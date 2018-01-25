package operator.router;

import java.util.Collection;
import java.util.List;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;

public class RouterOperatorDecorator<T extends Tuple> implements RouterOperator<T> {

	private final RouterOperator<T> decorated;
	private final ProcessCommandRouter<T> processingCommand = new ProcessCommandRouter<>(this);

	public RouterOperatorDecorator(RouterOperator<T> decorated) {
		this.decorated = decorated;
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
	public void run() {
		processingCommand.run();
	}

	@Override
	public void registerIn(StreamProducer<T> in) {
		decorated.registerIn(in);
	}

	@Override
	public Collection<StreamProducer<?>> getPrevious() {
		return decorated.getPrevious();
	}

	@Override
	public Stream<T> getInputStream(String requestorId) {
		return decorated.getInputStream(requestorId);
	}

	@Override
	public String getId() {
		return decorated.getId();
	}

	@Override
	public void addOutput(StreamConsumer<T> out) {
		decorated.addOutput(out);
	}

	@Override
	public Collection<StreamConsumer<T>> getNext() {
		return decorated.getNext();
	}

	@Override
	public Stream<T> getOutputStream(String requestorId) {
		return decorated.getOutputStream(requestorId);
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

}
