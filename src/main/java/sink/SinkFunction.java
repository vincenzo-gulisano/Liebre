package sink;

import common.Active;
import common.tuple.Tuple;

public interface SinkFunction<T extends Tuple> extends Active {
	void processTuple(T tuple);

	@Override
	default void enable() {
	}

	@Override
	default void disable() {

	}

	@Override
	default boolean isEnabled() {
		return true;
	}

}
