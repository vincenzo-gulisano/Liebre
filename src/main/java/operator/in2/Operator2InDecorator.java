package operator.in2;

import java.util.Collection;
import java.util.List;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import stream.Stream;

public class Operator2InDecorator<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		implements Operator2In<IN, IN2, OUT> {

	private final Operator2In<IN, IN2, OUT> decorated;

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
	public void registerIn2(StreamProducer<IN2> in) {
		decorated.registerIn2(in);
	}

	@Override
	public List<OUT> processTupleIn2(IN2 tuple) {
		return decorated.processTupleIn2(tuple);
	}

	@Override
	public Stream<IN2> getInput2Stream(String reqId) {
		return decorated.getInput2Stream(reqId);
	}

	@Override
	public Operator<IN2, OUT> secondInputView() {
		return decorated.secondInputView();
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
	public String toString() {
		return decorated.toString();
	}

}
