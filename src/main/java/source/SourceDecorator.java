package source;

import java.util.Collection;

import common.StreamConsumer;
import common.tuple.Tuple;
import stream.Stream;

public class SourceDecorator<T extends Tuple> implements Source<T> {

	private final Source<T> decorated;
	private final ProcessCommandSource<T> processCommand = new ProcessCommandSource<>(this);

	public SourceDecorator(Source<T> decorated) {
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
	public void run() {
		processCommand.run();
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
	public boolean hasOutput() {
		return decorated.hasOutput();
	}

	@Override
	public Stream<T> getOutputStream(String requestorId) {
		return decorated.getOutputStream(requestorId);
	}

	@Override
	public String getId() {
		return decorated.getId();
	}

	@Override
	public T getNextTuple() {
		return decorated.getNextTuple();
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
	public String toString() {
		return decorated.toString();
	}

}
