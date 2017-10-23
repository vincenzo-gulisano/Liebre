package common;

import common.tuple.Tuple;
import stream.Stream;

public interface StreamProducer<OUT extends Tuple> {
	void registerOut(String id, Stream<OUT> out);
}
