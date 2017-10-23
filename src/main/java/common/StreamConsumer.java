package common;

import common.tuple.Tuple;
import stream.Stream;

public interface StreamConsumer<IN extends Tuple> {
	void registerIn(String id, Stream<IN> in);
}
