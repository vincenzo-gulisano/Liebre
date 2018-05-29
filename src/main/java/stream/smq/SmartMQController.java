package stream.smq;

import common.Active;
import common.tuple.Tuple;
import common.util.backoff.Backoff;
import stream.Stream;

public interface SmartMQController extends Active {

  int register(Stream<? extends Tuple> stream, Backoff backoff);

  int register(Stream<? extends Tuple> stream);
}
