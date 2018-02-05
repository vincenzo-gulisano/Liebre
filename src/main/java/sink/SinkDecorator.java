package sink;

import java.util.Collection;

import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;

public class SinkDecorator<IN extends Tuple> implements Sink<IN> {

	private final Sink<IN> decorated;
	private final ProcessCommandSink<IN> processCommand = new ProcessCommandSink<>(this);

	public SinkDecorator(Sink<IN> decorated) {
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
	public void processTuple(IN tuple) {
		decorated.processTuple(tuple);
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
