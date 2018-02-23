package operator;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import common.BoxState;
import common.BoxState.BoxType;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

public abstract class AbstractOperator<IN extends Tuple, OUT extends Tuple> implements Operator<IN, OUT> {

	protected final BoxState<IN, OUT> state;

	public AbstractOperator(String id, BoxType type, StreamFactory streamFactory) {
		state = new BoxState<>(id, type, streamFactory);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return state.getNext();
	}

	@Override
	public Collection<StreamProducer<?>> getPrevious() {
		return state.getPrevious();
	}

	@Override
	public boolean hasInput() {
		return state.hasInput();
	}

	@Override
	public boolean hasOutput() {
		return state.hasOutput();
	}

	@Override
	public void enable() {
		state.enable();
	}

	@Override
	public void disable() {
		state.disable();
	}

	@Override
	public boolean isEnabled() {
		return state.isEnabled();
	}

	@Override
	public String getId() {
		return state.getId();
	}

	@Override
	public int getIndex() {
		return state.getIndex();
	}

	@Override
	public String toString() {
		return getId();
	}

	@Override
	public int hashCode() {
		return Objects.hash(state);
	}

	@Override
	public void onScheduled() {
		state.resetLog();
	}

	@Override
	public void onRun() {
	}

	@Override
	public Map<String, Long> getInputQueueDiff() {
		return state.getInputQueueDiff();
	}

	@Override
	public Map<String, Long> getOutputDiff() {
		return state.getOutputQueueDiff();
	}

	@Override
	public Map<String, Long> getLatencyLog() {
		return state.getLatencyLog();
	}

	@Override
	public void recordTupleRead(IN tuple, Stream<IN> input) {
		state.recordTupleRead(tuple, input);
	}

	@Override
	public void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		state.recordTupleWrite(tuple, output);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof AbstractOperator)) {
			return false;
		}
		AbstractOperator<?, ?> other = (AbstractOperator<?, ?>) obj;
		return Objects.equals(state, other.state);
	}

}