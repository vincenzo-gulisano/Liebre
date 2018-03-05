package operator.in2;

import java.util.Collection;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import scheduling.priority.PriorityMetric;
import stream.Stream;

class SecondInputOperator2InAdapter<IN extends Tuple, OUT extends Tuple> implements Operator<IN, OUT> {

	private final Operator2In<?, IN, OUT> decorated;

	public SecondInputOperator2InAdapter(Operator2In<?, IN, OUT> operator) {
		this.decorated = operator;
	}

	@Override
	public void run() {
		decorated.run();
	}

	@Override
	public void registerIn(StreamProducer<IN> in) {
		decorated.registerIn2(in);
	}

	@Override
	public Collection<StreamProducer<?>> getPrevious() {
		return decorated.getPrevious();
	}

	@Override
	public Stream<IN> getInputStream(String requestorId) {
		return decorated.getInput2Stream(requestorId);
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
	public boolean hasOutput() {
		return decorated.hasOutput();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((decorated == null) ? 0 : decorated.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		return this.decorated.equals(obj);
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
	public void onRun() {
		decorated.onRun();
	}

	@Override
	public void onScheduled() {
		decorated.onScheduled();
	}

	public void setPriorityMetric(PriorityMetric metric) {
		decorated.setPriorityMetric(metric);
	}

}
