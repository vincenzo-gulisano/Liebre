package sink;

import common.tuple.Tuple;

public interface SinkFunction<T extends Tuple> {
	Object processTuple(T tuple);
}
