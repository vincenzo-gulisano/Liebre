package common;

import common.tuple.Tuple;
import stream.Stream;

public interface StreamConsumer<IN extends Tuple> extends NamedEntity {
	void registerIn(StreamProducer<IN> in);

	Stream<IN> getInputStream(String reqId);

}
