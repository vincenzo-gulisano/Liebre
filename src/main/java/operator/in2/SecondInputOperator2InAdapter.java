package operator.in2;

import java.util.Collection;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import stream.Stream;

class SecondInputOperator2InAdapter<IN extends Tuple, OUT extends Tuple> implements Operator<IN, OUT> {

	private final Operator2In<?, IN, OUT> operator;

	public SecondInputOperator2InAdapter(Operator2In<?, IN, OUT> operator) {
		this.operator = operator;
	}

	@Override
	public void run() {
		operator.run();
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		operator.registerIn2(in);
	}

	@Override
	public Collection<StreamProducer<?>> getPrevious() {
		return operator.getPrevious();
	}

	@Override
	public Stream<IN> getInputStream(String requestorId) {
		return operator.getInput2Stream(requestorId);
	}

	@Override
	public String getId() {
		return operator.getId();
	}

	@Override
	public void addOutput(StreamConsumer<OUT> out) {
		operator.addOutput(out);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return operator.getNext();
	}

	@Override
	public Stream<OUT> getOutputStream(String requestorId) {
		return operator.getOutputStream(requestorId);
	}

	@Override
	public boolean hasInput() {
		return operator.hasInput();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		return this.operator.equals(obj);
	}

	@Override
	public void enable() {
		operator.enable();
	}

	@Override
	public boolean isEnabled() {
		return operator.isEnabled();
	}

	@Override
	public void disable() {
		operator.disable();
	}

}
