package source;

import java.util.Collection;

import common.StreamConsumer;
import common.tuple.Tuple;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class SourceDecorator<OUT extends Tuple> implements Source<OUT> {

	private final Source<OUT> decorated;
	private final ProcessCommandSource<OUT> processCommand = new ProcessCommandSource<>(this);

	public SourceDecorator(Source<OUT> decorated) {
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
	public void addOutput(StreamConsumer<OUT> out) {
		decorated.addOutput(out);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return decorated.getNext();
	}

	@Override
	public boolean hasOutput() {
		return decorated.hasOutput();
	}

	@Override
	public Stream<OUT> getOutputStream(String requestorId) {
		return decorated.getOutputStream(requestorId);
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
	public OUT getNextTuple() {
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
	public void setPriorityMetric(PriorityMetric metric) {
		decorated.setPriorityMetric(metric);
	}

	@Override
	public String toString() {
		return decorated.toString();
	}

}
