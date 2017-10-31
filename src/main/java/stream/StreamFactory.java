package stream;

import common.tuple.Tuple;

public interface StreamFactory {
	<T extends Tuple> Stream<T> newStream(String fromId, String toId);
}
