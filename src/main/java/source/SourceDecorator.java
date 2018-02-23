package source;

import java.util.Collection;
import java.util.Map;

import common.StreamConsumer;
import common.tuple.Tuple;
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
	public String toString() {
		return decorated.toString();
	}

	@Override
	public Map<String, Long> getOutputQueueDiff() {
		return decorated.getOutputQueueDiff();
	}

	@Override
	public Map<String, Long> getLatencyLog() {
		return decorated.getLatencyLog();
	}

	public void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		decorated.recordTupleWrite(tuple, output);
	}

	public Map<String, Long> getInputQueueDiff() {
		return decorated.getInputQueueDiff();
	}

}
