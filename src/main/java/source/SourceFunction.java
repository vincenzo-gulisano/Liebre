package source;

import common.Active;
import common.tuple.Tuple;

public interface SourceFunction<OUT extends Tuple> extends Active {
	public abstract OUT getNextTuple();

}
