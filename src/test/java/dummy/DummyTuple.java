package dummy;

import common.tuple.RichTuple;

/**
 * Dummy tuple which only has a timestamp.
 * 
 * @author palivosd
 *
 */
public class DummyTuple implements RichTuple {

	long ts;

	public DummyTuple(long ts) {
		this.ts = ts;
	}

	public String getKey() {
		return null;
	}

	public long getTimestamp() {
		return ts;
	}

}
