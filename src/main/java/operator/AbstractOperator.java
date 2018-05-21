package operator;

import java.util.Collection;
import java.util.Objects;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ComponentState;
import common.component.ComponentState.BoxType;
import common.tuple.Tuple;
import stream.StreamFactory;

public abstract class AbstractOperator<IN extends Tuple, OUT extends Tuple> implements Operator<IN, OUT> {

	protected final ComponentState<IN, OUT> state;

	public AbstractOperator(String id, BoxType type, StreamFactory streamFactory) {
		state = new ComponentState<>(id, type, streamFactory);
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
	}

	@Override
	public void onRun() {
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