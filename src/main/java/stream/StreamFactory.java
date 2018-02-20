package stream;

import common.NamedEntity;
import common.tuple.Tuple;

public interface StreamFactory {
	<T extends Tuple> Stream<T> newStream(NamedEntity from, NamedEntity to);
}
