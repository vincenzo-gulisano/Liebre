package source;

import common.tuple.Tuple;

public interface SourceFunction<OUT extends Tuple> {
	public abstract OUT getNextTuple();

}
