package dummy;

import common.util.Util;
import source.SourceFunction;

public class DummySourceFunction implements SourceFunction<DummyTuple> {

	private final long sleep;

	public DummySourceFunction(long sleep) {
		this.sleep = sleep;

	}

	@Override
	public DummyTuple getNextTuple() {
		Util.sleep(sleep);
		return new DummyTuple(System.nanoTime());
	}
}
