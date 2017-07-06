package operator.aggregate;

import tuple.RichTuple;

public abstract class BaseTimeBasedSingleWindow<T1 extends RichTuple, T2 extends RichTuple>
		implements TimeBasedSingleWindow<T1, T2> {

	protected String key;
	protected long startTimestamp;

	@Override
	public abstract TimeBasedSingleWindow<T1, T2> factory();

	@Override
	public abstract void add(T1 t);

	@Override
	public abstract void remove(T1 t);

	@Override
	public abstract T2 getAggregatedResult();

	@Override
	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public void setStartTimestamp(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}

}
