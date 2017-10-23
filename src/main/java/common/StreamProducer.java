package common;

import stream.Stream;
import tuple.Tuple;

public interface StreamProducer<OUT extends Tuple> {
	void registerOut(String id, Stream<OUT> out);
}
