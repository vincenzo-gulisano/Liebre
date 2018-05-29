package stream;

import common.component.Component;
import common.tuple.Tuple;

public class StreamDecorator<T extends Tuple> implements Stream<T> {

	private final Stream<T> decorated;

	public StreamDecorator(Stream<T> decorated) {
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
	public String getId() {
		return decorated.getId();
	}

	@Override
	public int getIndex() {
		return decorated.getIndex();
	}

	@Override
	public void addTuple(T tuple) {
		decorated.addTuple(tuple);
	}

	@Override
	public T getNextTuple() {
		return decorated.getNextTuple();
	}

	@Override
	public T peek() {
		return decorated.peek();
	}

	@Override
	public long size() {
		return decorated.size();
	}

	@Override
	public long remainingCapacity() {
		return decorated.remainingCapacity();
	}

	@Override
	public boolean offer(T tuple) {
		return decorated.offer(tuple);
	}

	@Override
	public T poll() {
		return decorated.poll();
	}

	@Override
	public String toString() {
		return decorated.toString();
	}

	public Component getSource() {
		return decorated.getSource();
	}

	public Component getDestination() {
		return decorated.getDestination();
	}

}
