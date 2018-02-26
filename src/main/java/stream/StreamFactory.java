package stream;

import common.ActiveRunnable;
import common.tuple.Tuple;

public interface StreamFactory {
	<T extends Tuple> Stream<T> newStream(ActiveRunnable from, ActiveRunnable to);
}
