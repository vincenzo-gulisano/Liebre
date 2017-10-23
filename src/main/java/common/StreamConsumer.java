package common;

import stream.Stream;
import tuple.Tuple;

public interface StreamConsumer<IN extends Tuple> {
	void registerIn(String id, Stream<IN> in);
}
