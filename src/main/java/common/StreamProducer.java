package common;

import java.util.Collection;

import common.tuple.Tuple;
import stream.Stream;

public interface StreamProducer<OUT extends Tuple> extends NamedEntity {
	void registerOut(StreamConsumer<OUT> out);

	Collection<StreamConsumer<OUT>> getNext();

	Stream<OUT> getOutputStream(String reqId);

}
