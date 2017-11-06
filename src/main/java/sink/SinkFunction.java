package sink;

import common.Active;
import common.tuple.Tuple;

public interface SinkFunction<T extends Tuple> extends Active {
	void processTuple(T tuple);
}
