package operator;

import java.util.Collection;
import java.util.List;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;

public class OperatorDecorator<IN extends Tuple, OUT extends Tuple> implements Operator1In<IN, OUT> {

	private final Operator1In<IN, OUT> decorated;

	public OperatorDecorator(Operator1In<IN, OUT> decorated) {
		this.decorated = decorated;
	}

	@Override
	public void run() {
		decorated.run();
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		decorated.registerIn(in);
	}

	@Override
	public Collection<StreamProducer<?>> getPrevious() {
		return decorated.getPrevious();
	}

	@Override
	public Stream<IN> getInputStream(String requestorId) {
		return decorated.getInputStream(requestorId);
	}

	@Override
	public String getId() {
		return decorated.getId();
	}

	@Override
	public void addOutput(StreamConsumer<OUT> out) {
		decorated.addOutput(out);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return decorated.getNext();
	}

	@Override
	public Stream<OUT> getOutputStream(String requestorId) {
		return decorated.getOutputStream(requestorId);
	}

	@Override
	public boolean hasInput() {
		return decorated.hasInput();
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

}
