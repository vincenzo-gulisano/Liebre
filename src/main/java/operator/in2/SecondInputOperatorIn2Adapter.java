package operator.in2;

import java.util.Collection;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import stream.Stream;

class SecondInputOperatorIn2Adapter<IN extends Tuple, OUT extends Tuple> implements Operator<IN, OUT> {

	private final Operator2In<?, IN, OUT> operator;

	public SecondInputOperatorIn2Adapter(Operator2In<?, IN, OUT> operator) {
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
	public void registerOut(StreamConsumer<OUT> out) {
		operator.registerOut(out);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return operator.getNext();
	}

	@Override
	public Stream<OUT> getOutputStream(String requestorId) {
		return operator.getOutputStream(requestorId);
	}

}
